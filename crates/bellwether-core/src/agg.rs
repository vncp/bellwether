//! Agg keys (B7): per-key, per-row-group aggregates of one value column.
//!
//! An `agg:<key>@<value>{sum,count,min,max}` key stores, for every key and
//! every row group holding it, one [`AggCell`]: the rows of the key there,
//! the rows whose value is null (ADR 014: nulls are counted, never summed or
//! compared), and, as declared, the sum and the minimum and maximum of the
//! non-null values. The cells ride in the keyed layout's value records
//! (`docs/format.md`, "Agg keys"), after the bitmap, one per set bit in
//! ascending row-group order.
//!
//! Values are read at the dataset type, through the canonical encoding
//! (ADR 016): a decimal is rescaled to the dataset scale exactly or the row
//! group is refused, so a sum of a decimal column is the exact `i128` at the
//! declared scale; integers sum exactly as `i128`; floats sum as a
//! Kahan-Babuska (Neumaier) compensated pair, which is never exact and is
//! labelled [`bellwether_types::AGG_FLOAT_SUM_MODEL`] wherever it prints.
//! Minimum and maximum compare canonical bytes, which order as the typed
//! values do: `-0.0 < +0.0`, and NaN is the greatest float.
//!
//! [`cover`] decides, per lookup key, which row groups an agg key answers:
//! a cell covers a matching row group when its rows equal the rows the
//! lookup's counted exact key records there (the counts prove every row of
//! the agg key in that row group is a row of the lookup key). Every other
//! matching row group is residual: the lookup returns the exact partial
//! aggregate and the row groups still to read.

use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Schema};
use bellwether_types::{AggregateFn, Error, KeySpec, Result};
use roaring::RoaringBitmap;

use crate::encoding::{self, tag, width};
use crate::segment::{corrupt, put_leb128, Cursor, Put};

/// Tag of an integer sum in a cell: an `i128`, little-endian.
const SUM_INT: u8 = 0x01;
/// Tag of a float sum in a cell: the running sum and its compensation, two
/// `f64`, little-endian.
const SUM_FLOAT: u8 = 0x02;

/// How a value column is summed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SumKind {
    /// Exactly, as an `i128`: an integer column, or a `Decimal128` column at
    /// its dataset scale.
    Int,
    /// As a compensated `f64` pair: a float column. Never exact.
    Float,
}

/// The sum kind of a dataset type, or the refusal that says why its values
/// cannot be summed.
pub fn sum_kind(column: &str, dt: &DataType) -> Result<SumKind> {
    match dt {
        DataType::Dictionary(_, v) => sum_kind(column, v),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Decimal128(..) => Ok(SumKind::Int),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok(SumKind::Float),
        DataType::Decimal256(..) => Err(Error::KeySpec(format!(
            "{column} is {dt}, and an agg key sums into a 128-bit integer, which cannot hold a \
             Decimal256 total exactly; declare the agg key without sum (for example \
             {{count,min,max}})"
        ))),
        other => Err(Error::KeySpec(format!(
            "{column} is {other}, which has no sum; an agg key sums integer, decimal, and float \
             columns only, so declare it without sum (for example {{count,min,max}})"
        ))),
    }
}

/// Checks the parts of an agg spec that need no schema: at least one key
/// column, a value column that is not a key column, and functions in
/// canonical order with `count` among them. Other specs pass.
pub fn check_spec(spec: &KeySpec) -> Result<()> {
    let KeySpec::Agg {
        columns,
        value,
        functions,
    } = spec
    else {
        return Ok(());
    };
    if value.is_empty() {
        return Err(Error::KeySpec(
            "an agg key needs a value column: agg:<key>@<value>{sum,count,min,max}".into(),
        ));
    }
    if columns.contains(value) {
        return Err(Error::KeySpec(format!(
            "agg key {}: the value column {value} is also a key column; aggregate another \
             column, or count the key with exact:{},counts",
            spec.name(),
            columns.join(",")
        )));
    }
    if *functions != AggregateFn::canonical(functions) || functions.is_empty() {
        return Err(Error::KeySpec(format!(
            "agg key {}: functions must be listed once each in the order sum, count, min, max, \
             with count always among them; found {{{}}}",
            spec.name(),
            functions
                .iter()
                .map(|f| f.as_str())
                .collect::<Vec<_>>()
                .join(",")
        )));
    }
    Ok(())
}

/// The value column's dataset type of an agg spec, checked against the
/// schema (it exists, has a canonical encoding, and can be summed when the
/// spec stores sums); `None` for other specs.
pub fn value_type(spec: &KeySpec, schema: &Schema) -> Result<Option<DataType>> {
    let KeySpec::Agg {
        value, functions, ..
    } = spec
    else {
        return Ok(None);
    };
    check_spec(spec)?;
    let dt = schema
        .field_with_name(value)
        .map_err(|_| Error::KeySpec(format!("column {value} is not in the dataset schema")))?
        .data_type()
        .clone();
    if encoding::nested_kind(&dt).is_some() {
        return Err(Error::KeySpec(format!(
            "agg key {}: the value column {value} is {dt}, a nested column with no single value \
             per row to aggregate",
            spec.name()
        )));
    }
    encoding::check_supported(&dt)?;
    if functions.contains(&AggregateFn::Sum) {
        sum_kind(value, &dt)?;
    }
    Ok(Some(dt))
}

/// A sum stored in a cell.
#[derive(Clone, Copy, Debug)]
pub enum AggSum {
    /// An exact sum: integers, or a decimal at its dataset scale.
    Int(i128),
    /// A compensated float sum; the value is `sum + comp`. Never exact.
    /// Once the running sum is infinite or NaN it stays as IEEE 754 makes
    /// it (`inf + 1 = inf`, `inf + -inf = NaN`) and the compensation stops.
    Float {
        /// The running sum.
        sum: f64,
        /// The running compensation.
        comp: f64,
    },
}

/// Two sums are equal when their stored bytes are: integers by value,
/// floats bit for bit, so a NaN sum equals the same NaN and `-0.0` differs
/// from `+0.0`, as the cells a build writes do.
impl PartialEq for AggSum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Float { sum: a, comp: c }, Self::Float { sum: b, comp: d }) => {
                a.to_bits() == b.to_bits() && c.to_bits() == d.to_bits()
            }
            _ => false,
        }
    }
}

/// The text of the error an integer sum past 128 bits raises in
/// [`AggCell::add_row`] and [`AggCell::combine`], which know no spec or
/// key; callers that do replace it with [`sum_overflow`].
const SUM_OVERFLOW: &str = "an agg sum overflows a 128-bit integer";

/// Whether `e` is the bare overflow of an integer sum, before a caller
/// names the agg key and key ([`sum_overflow`]).
#[must_use]
pub fn is_sum_overflow(e: &Error) -> bool {
    matches!(e, Error::Other(m) if m == SUM_OVERFLOW)
}

/// The functions of `spec` without `sum`, as a spec lists them:
/// `{count,min,max}`.
fn without_sum(spec: &KeySpec) -> String {
    let rest: Vec<&str> = functions(spec)
        .iter()
        .filter(|f| **f != AggregateFn::Sum)
        .map(|f| f.as_str())
        .collect();
    format!("{{{}}}", rest.join(","))
}

/// Where a sum overflowed: within one row group as the build writes its
/// cell, or across the row groups a lookup adds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverflowAt {
    /// The build, in this row group.
    RowGroup(u32),
    /// A lookup, adding the key's cells.
    Lookup,
}

/// The refusal for a sum of `spec`'s value column for the key `key` (as
/// displayed) past 128 bits: what overflowed, where, and what to do.
#[must_use]
pub fn sum_overflow(spec: &KeySpec, key: &str, at: OverflowAt) -> Error {
    let value = match spec {
        KeySpec::Agg { value, .. } => value.as_str(),
        _ => "the value",
    };
    let rest = without_sum(spec);
    match at {
        OverflowAt::RowGroup(rg) => Error::KeySpec(format!(
            "{}: the sum of {value} for key ({key}) in row group {rg} overflows a 128-bit \
             integer, so no exact sum can be stored; declare the agg key without sum \
             (agg:{}@{value}{rest})",
            spec.identity(),
            spec.columns().join(","),
        )),
        OverflowAt::Lookup => Error::KeySpec(format!(
            "{}: the sum of {value} for key ({key}) over its row groups overflows a 128-bit \
             integer, so it has no exact total; ask without sum (--fn {}), which this key \
             answers",
            spec.identity(),
            rest.trim_start_matches('{').trim_end_matches('}'),
        )),
    }
}

impl AggSum {
    fn zero(kind: SumKind) -> Self {
        match kind {
            SumKind::Int => Self::Int(0),
            // -0.0 is the identity of IEEE 754 addition: a sum of only
            // -0.0 values stays -0.0, and -0.0 + 0.0 is 0.0.
            SumKind::Float => Self::Float {
                sum: -0.0,
                comp: 0.0,
            },
        }
    }

    /// Adds `other`, as the next term: an integer sum exactly (an overflow
    /// is an error, [`is_sum_overflow`]), a float sum by Neumaier's rule on
    /// its running sum, its compensation added to ours while both are
    /// finite.
    fn add(&mut self, other: &Self) -> Result<()> {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => {
                *a = a
                    .checked_add(*b)
                    .ok_or_else(|| Error::Other(SUM_OVERFLOW.into()))?;
                Ok(())
            }
            (Self::Float { sum, comp }, Self::Float { sum: s, comp: c }) => {
                neumaier(sum, comp, *s);
                if sum.is_finite() {
                    *comp += c;
                }
                Ok(())
            }
            _ => Err(corrupt("an agg key mixes integer and float sums")),
        }
    }

    /// The sum as a decimal string: an integer, or a decimal at `scale`
    /// (`Decimal128`), or a float as Arrow displays a `Float64` (`0.0`,
    /// `-0.0`, `6.5`, `inf`, `NaN`), as a float minimum and maximum print.
    #[must_use]
    pub fn render(&self, scale: Option<i8>) -> String {
        match self {
            Self::Int(v) => render_scaled(*v, scale.unwrap_or(0)),
            Self::Float { .. } => crate::render::render_f64(self.value()),
        }
    }

    /// The sum's value as an `f64`: `sum + comp` for a float sum, or the
    /// sum alone when there is no compensation (so a `-0.0` sum keeps its
    /// sign) or it is not finite.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn value(&self) -> f64 {
        match self {
            Self::Int(v) => *v as f64,
            Self::Float { sum, comp } if sum.is_finite() && *comp != 0.0 => sum + comp,
            Self::Float { sum, .. } => *sum,
        }
    }

    /// Whether the sum is exact (integers and decimals).
    #[must_use]
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Int(_))
    }
}

/// Neumaier's compensated addition of `x` to `(sum, comp)`. Once the sum is
/// not finite (an infinite or NaN term, or a finite overflow) it is kept
/// as IEEE 754 makes it and the compensation stops: `(sum - t) + x` of an
/// infinite `t` is NaN, which would turn `inf + 1` into NaN.
fn neumaier(sum: &mut f64, comp: &mut f64, x: f64) {
    let t = *sum + x;
    if t.is_finite() {
        if sum.abs() >= x.abs() {
            *comp += (*sum - t) + x;
        } else {
            *comp += (x - t) + *sum;
        }
    }
    *sum = t;
}

/// `v` at decimal `scale` as a string: `-12.340` for `v = -12340, scale =
/// 3`; a negative scale appends zeros.
#[must_use]
pub fn render_scaled(v: i128, scale: i8) -> String {
    if scale <= 0 {
        let zeros = "0".repeat(usize::from(scale.unsigned_abs()));
        return if v == 0 {
            "0".into()
        } else {
            format!("{v}{zeros}")
        };
    }
    let s = usize::from(scale.unsigned_abs());
    let digits = v.unsigned_abs().to_string();
    let digits = if digits.len() <= s {
        format!("{}{digits}", "0".repeat(s + 1 - digits.len()))
    } else {
        digits
    };
    let (int, frac) = digits.split_at(digits.len() - s);
    format!("{}{int}.{frac}", if v < 0 { "-" } else { "" })
}

/// The numeric value of a canonical encoding, for a sum: an integer or a
/// `Decimal128` (at the scale it was encoded at) as an `i128`, a float as
/// an `f64`.
fn summand(value: &[u8]) -> Result<AggSum> {
    let be8 = |at: usize| -> Result<u64> {
        let b: [u8; 8] = value
            .get(at..at + 8)
            .and_then(|b| b.try_into().ok())
            .ok_or_else(|| corrupt("a value encoding is truncated"))?;
        Ok(u64::from_be_bytes(b))
    };
    match value.first() {
        Some(&tag::INT) => {
            #[allow(clippy::cast_possible_wrap)]
            let v = (be8(1)? ^ (1 << 63)) as i64;
            Ok(AggSum::Int(i128::from(v)))
        }
        Some(&tag::UINT) => Ok(AggSum::Int(i128::from(be8(1)?))),
        Some(&tag::FLOAT) => {
            let stored = be8(1)?;
            let bits = if stored & (1 << 63) != 0 {
                stored ^ (1 << 63)
            } else {
                !stored
            };
            Ok(AggSum::Float {
                sum: f64::from_bits(bits),
                comp: 0.0,
            })
        }
        Some(&tag::DECIMAL) if value.get(1) == Some(&width::DECIMAL128) => {
            let mut raw: [u8; 16] = value
                .get(2..18)
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| corrupt("a decimal encoding is truncated"))?;
            raw[0] ^= 0x80;
            Ok(AggSum::Int(i128::from_be_bytes(raw)))
        }
        _ => Err(Error::KeySpec(
            "an agg key sums integer, Decimal128, and float values only; this value is of \
             another type, so declare the agg key without sum"
                .into(),
        )),
    }
}

/// One key's aggregates in one row group (or, combined, over several).
#[derive(Clone, Debug, PartialEq)]
pub struct AggCell {
    /// Rows of the key, at least 1 in a stored cell.
    pub rows: u64,
    /// Rows of the key whose value is null, at most `rows` (ADR 014).
    pub nulls: u64,
    /// The sum of the non-null values, when the spec stores sums (0 when
    /// every value is null).
    pub sum: Option<AggSum>,
    /// The least non-null value's canonical encoding, when the spec stores
    /// minimums and a value is not null.
    pub min: Option<Vec<u8>>,
    /// The greatest non-null value's canonical encoding, likewise.
    pub max: Option<Vec<u8>>,
}

impl AggCell {
    /// An empty cell for `functions` (no row yet).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            rows: 0,
            nulls: 0,
            sum: None,
            min: None,
            max: None,
        }
    }

    /// Non-null values: `count(value)`.
    #[must_use]
    pub fn values(&self) -> u64 {
        self.rows - self.nulls
    }

    /// Adds one row whose value has canonical encoding `value` (`None` for
    /// null) under `functions`. On error the cell is unchanged.
    pub fn add_row(&mut self, value: Option<&[u8]>, functions: &[AggregateFn]) -> Result<()> {
        let Some(v) = value else {
            self.rows += 1;
            self.nulls += 1;
            if functions.contains(&AggregateFn::Sum) && self.sum.is_none() {
                // The sum's kind is unknown until a value is seen; a cell
                // of nulls stores an integer zero (format.md).
                self.sum = Some(AggSum::Int(0));
            }
            return Ok(());
        };
        if functions.contains(&AggregateFn::Sum) {
            let x = summand(v)?;
            let mut sum = match (self.sum, x) {
                (None | Some(AggSum::Int(0)), AggSum::Float { .. }) if self.values() == 0 => {
                    AggSum::zero(SumKind::Float)
                }
                (None, _) => AggSum::zero(SumKind::Int),
                (Some(s), _) => s,
            };
            match (&mut sum, x) {
                (AggSum::Float { sum, comp }, AggSum::Float { sum: x, .. }) => {
                    neumaier(sum, comp, x);
                }
                (s, x) => s.add(&x)?,
            }
            self.sum = Some(sum);
        }
        if functions.contains(&AggregateFn::Min) && self.min.as_deref().is_none_or(|m| v < m) {
            self.min = Some(v.to_vec());
        }
        if functions.contains(&AggregateFn::Max) && self.max.as_deref().is_none_or(|m| v > m) {
            self.max = Some(v.to_vec());
        }
        self.rows += 1;
        Ok(())
    }

    /// Folds `other` (another row group's cell of the same key) into this
    /// one: rows and nulls added, sums added as the next term, the least
    /// minimum and greatest maximum kept.
    pub fn combine(&mut self, other: &Self) -> Result<()> {
        self.rows = self
            .rows
            .checked_add(other.rows)
            .ok_or_else(|| Error::Other("agg rows overflow u64".into()))?;
        self.nulls += other.nulls;
        self.sum = match (self.sum, &other.sum) {
            (None, s) => *s,
            (s, None) => s,
            // A cell of only nulls carries an integer zero; it adds nothing
            // to a float sum.
            (Some(AggSum::Int(0)), Some(f @ AggSum::Float { .. })) => Some(*f),
            (Some(f @ AggSum::Float { .. }), Some(AggSum::Int(0))) => Some(f),
            (Some(mut a), Some(b)) => {
                a.add(b)?;
                Some(a)
            }
        };
        for (mine, theirs, less) in [
            (&mut self.min, &other.min, true),
            (&mut self.max, &other.max, false),
        ] {
            if let Some(t) = theirs {
                let replace = match mine.as_ref() {
                    None => true,
                    Some(m) if less => t < m,
                    Some(m) => t > m,
                };
                if replace {
                    *mine = Some(t.clone());
                }
            }
        }
        Ok(())
    }

    /// The cell's bytes: `rows LEB128 | nulls LEB128 | [sum] | [min] |
    /// [max]` (`docs/format.md`, "Agg keys"). `sum` is present exactly when
    /// the spec stores sums; `min` and `max` exactly when the spec stores
    /// them and some value is not null.
    pub(crate) fn encode(&self, shape: CellShape, out: &mut Vec<u8>) -> Result<()> {
        if self.rows == 0 || self.nulls > self.rows {
            return Err(Error::Other(
                "an agg cell holds at least one row and at most that many nulls".into(),
            ));
        }
        put_leb128(out, self.rows);
        put_leb128(out, self.nulls);
        if shape.sum {
            match self.sum {
                Some(AggSum::Int(v)) => {
                    out.put_u8(SUM_INT);
                    out.extend_from_slice(&v.to_le_bytes());
                }
                Some(AggSum::Float { sum, comp }) => {
                    out.put_u8(SUM_FLOAT);
                    out.extend_from_slice(&sum.to_bits().to_le_bytes());
                    out.extend_from_slice(&comp.to_bits().to_le_bytes());
                }
                None => return Err(Error::Other("an agg cell lacks its declared sum".into())),
            }
        }
        let valued = self.values() > 0;
        for (f, stored, v) in [
            (AggregateFn::Min, shape.min, &self.min),
            (AggregateFn::Max, shape.max, &self.max),
        ] {
            if stored && valued {
                let b = v.as_ref().ok_or_else(|| {
                    Error::Other(format!("an agg cell with values lacks its {}", f.as_str()))
                })?;
                out.put_bytes(b)?;
            }
        }
        Ok(())
    }

    /// Reads `n` cells written by [`AggCell::encode`] for `shape`, one per
    /// row group of a value record's bitmap; `None` without a shape (not an
    /// agg key's record). Allocates as cells are read, never `n` up front
    /// (`n` comes from the bytes).
    pub(crate) fn decode_n(
        c: &mut Cursor<'_>,
        n: usize,
        shape: Option<CellShape>,
    ) -> Result<Option<Vec<Self>>> {
        let Some(shape) = shape else {
            return Ok(None);
        };
        let mut v = Vec::new();
        for _ in 0..n {
            v.push(Self::decode(c, shape)?);
        }
        Ok(Some(v))
    }

    /// Reads one cell written by [`AggCell::encode`] for `shape`.
    /// Anything malformed is refused as corruption.
    pub(crate) fn decode(c: &mut Cursor<'_>, shape: CellShape) -> Result<Self> {
        let rows = c.leb128()?;
        let nulls = c.leb128()?;
        if rows == 0 || nulls > rows {
            return Err(corrupt(
                "an agg cell holds no rows, or more nulls than rows",
            ));
        }
        let sum = if shape.sum {
            Some(match c.u8()? {
                SUM_INT => {
                    let b: [u8; 16] = c.take(16)?.try_into().map_err(|_| corrupt("agg sum"))?;
                    AggSum::Int(i128::from_le_bytes(b))
                }
                SUM_FLOAT => {
                    let s = c.u64()?;
                    let k = c.u64()?;
                    AggSum::Float {
                        sum: f64::from_bits(s),
                        comp: f64::from_bits(k),
                    }
                }
                _ => return Err(corrupt("unknown agg sum tag")),
            })
        } else {
            None
        };
        let valued = rows > nulls;
        let mut read = |stored: bool| -> Result<Option<Vec<u8>>> {
            if stored && valued {
                let b = c.bytes()?;
                if b.is_empty() {
                    return Err(corrupt("an empty agg minimum or maximum"));
                }
                Ok(Some(b.to_vec()))
            } else {
                Ok(None)
            }
        };
        let min = read(shape.min)?;
        let max = read(shape.max)?;
        if let (Some(a), Some(b)) = (&min, &max) {
            if a > b {
                return Err(corrupt("an agg minimum above its maximum"));
            }
        }
        Ok(Self {
            rows,
            nulls,
            sum,
            min,
            max,
        })
    }
}

/// What an agg key's cells hold after `rows | nulls`: a sum, a minimum, a
/// maximum, each exactly when the spec stores that function (`count` is
/// `rows - nulls`, always present). Part of [`crate::segment::ValueShape`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CellShape {
    /// The cell holds a sum.
    pub sum: bool,
    /// The cell holds a minimum when some value is not null.
    pub min: bool,
    /// The cell holds a maximum when some value is not null.
    pub max: bool,
}

impl CellShape {
    /// The cell shape of an agg key storing `functions`.
    #[must_use]
    pub fn of(functions: &[AggregateFn]) -> Self {
        Self {
            sum: functions.contains(&AggregateFn::Sum),
            min: functions.contains(&AggregateFn::Min),
            max: functions.contains(&AggregateFn::Max),
        }
    }
}

/// The functions an agg spec stores (empty for other specs).
#[must_use]
pub fn functions(spec: &KeySpec) -> &[AggregateFn] {
    match spec {
        KeySpec::Agg { functions, .. } => functions,
        _ => &[],
    }
}

/// One lookup key's aggregate under one agg key ([`cover`]).
#[derive(Clone, Debug, PartialEq)]
pub struct KeyCover {
    /// Rows of the lookup key in live row groups, from the counted exact
    /// key: exact whatever the agg key covers.
    pub rows: u64,
    /// Live row groups holding the lookup key.
    pub row_groups: RoaringBitmap,
    /// The aggregate over the covered row groups, combined in ascending
    /// row-group order; `None` when none is covered.
    pub covered: Option<AggCell>,
    /// Matching row groups the agg key does not answer: read them to finish
    /// the aggregate.
    pub residual: RoaringBitmap,
    /// Rows of the lookup key in the residual row groups.
    pub residual_rows: u64,
}

/// The first of [`cover`]'s two steps: the matching row groups of one
/// lookup key that the agg key cannot answer. `counts` are the lookup
/// key's rows per live row group (the counted exact key on the lookup's
/// columns, every variant of the key summed); `cells` the agg key's cells
/// per live row group (every variant of the key's projection onto the agg
/// key's columns, combined). `same` says the agg key's columns are the
/// lookup's, so each cell must hold exactly the counted rows.
///
/// A matching row group is answerable when its cell holds as many rows as
/// the lookup key has there: every row of the agg key in it is then a row
/// of the lookup key. A cell holding more rows (a finer lookup key over a
/// coarser agg key) leaves the row group residual. A cell holding fewer
/// rows, or none, for a row group the complete agg key processed is a
/// corrupt index, refused, never a wrong total.
pub fn unanswered(
    counts: &BTreeMap<u32, u64>,
    cells: &BTreeMap<u32, AggCell>,
    same: bool,
    spec: &KeySpec,
) -> Result<RoaringBitmap> {
    let mut out = RoaringBitmap::new();
    for (&rg, &n) in counts {
        let Some(cell) = cells.get(&rg) else {
            return Err(Error::Other(format!(
                "corrupt index: {} holds no aggregate for a key in row group {rg}, which its \
                 counted exact key says holds {n} rows of it; rebuild the agg key",
                spec.identity()
            )));
        };
        if cell.rows < n || (same && cell.rows != n) {
            return Err(Error::Other(format!(
                "corrupt index: {} aggregates {} rows of a key in row group {rg}, and its counted \
                 exact key counts {n}; rebuild both keys",
                spec.identity(),
                cell.rows
            )));
        }
        if cell.rows != n {
            out.insert(rg);
        }
    }
    Ok(out)
}

/// The second step: one lookup key's aggregate when the row groups in
/// `residual` are read instead of answered, for every key of the lookup
/// (the union of every key's [`unanswered`] row groups), so that reading
/// them with the lookup's keys as the residual predicate adds exactly the
/// rows the partial aggregate lacks. Cells are combined in ascending
/// row-group order.
pub fn cover(
    counts: &BTreeMap<u32, u64>,
    cells: &BTreeMap<u32, AggCell>,
    residual: &RoaringBitmap,
) -> Result<KeyCover> {
    let mut out = KeyCover {
        rows: 0,
        row_groups: RoaringBitmap::new(),
        covered: None,
        residual: RoaringBitmap::new(),
        residual_rows: 0,
    };
    for (&rg, &n) in counts {
        out.rows = out
            .rows
            .checked_add(n)
            .ok_or_else(|| Error::Other("row count overflows u64".into()))?;
        out.row_groups.insert(rg);
        if residual.contains(rg) {
            out.residual.insert(rg);
            out.residual_rows += n;
            continue;
        }
        let cell = cells
            .get(&rg)
            .ok_or_else(|| corrupt("an answered row group without its aggregate"))?;
        match out.covered.as_mut() {
            Some(c) => c.combine(cell)?,
            None => out.covered = Some(cell.clone()),
        }
    }
    Ok(out)
}

/// Encodes `array[row]` at the value column's dataset type for an agg cell:
/// `None` for a null.
pub fn value_at(
    array: &dyn arrow::array::Array,
    row: usize,
    declared: &DataType,
    buf: &mut Vec<u8>,
) -> Result<bool> {
    buf.clear();
    encoding::encode_array_value_as(array, row, declared, buf)
}
