//! `aggregate` (B7): `sum`, `count`, `min`, `max` of one column per key over
//! a key set, from the index's `agg` keys, with the row groups still to
//! read when the index answers only part.

use arrow::datatypes::DataType;
use bellwether_build::{AggLookup, Aggregated};
use bellwether_core::agg::{AggCell, AggSum};
use bellwether_types::{AggregateFn, Error, Exactness, Hash, KeySpec, Result, AGG_FLOAT_SUM_MODEL};
use bellwether_verify::render_tuple;
use serde::{Deserialize, Serialize};

use crate::engine::{guard, schema_hint, Engine};
use crate::keys::KeyFile;

/// What [`Engine::aggregate`] answered: per key, its rows and the asked
/// aggregates of `value` over the row groups the index answers, and the
/// row groups still to read. Reads the manifest and index segments only;
/// writes nothing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregateResult {
    /// Key columns, in tuple order.
    pub columns: Vec<String>,
    /// The aggregated column.
    pub value: String,
    /// The functions asked, in the order sum, count, min, max.
    pub functions: Vec<String>,
    /// Distinct non-null key tuples asked.
    pub keys: u64,
    /// Input rows dropped because their tuple held a null (ADR 014).
    pub null_tuples: u64,
    /// BLAKE3 of the canonical key set (the one `locate` cites).
    pub key_set: Hash,
    /// The manifest sequence answered against.
    pub sequence: u64,
    /// The agg key that answered (its identity).
    pub answered_by: String,
    /// The spec whose per-key counts proved which aggregates answer.
    pub counted_by: String,
    /// Whether every key is answered in full: no row group to read.
    pub complete: bool,
    /// Rows holding the keys, exact (from the counts).
    pub rows: u64,
    /// Live row groups holding the keys.
    pub row_groups: u64,
    /// Live row groups in the index.
    pub row_groups_total: u64,
    /// Row groups still to read to finish the aggregates (0 when
    /// complete): read them, keep the rows of the keys, and fold them in.
    pub residual_row_groups: u64,
    /// Compressed bytes of those row groups.
    pub residual_bytes: u64,
    /// Files holding them.
    pub residual_files: u64,
    /// One entry per key asked, in canonical key order.
    pub groups: Vec<AggregateGroup>,
}

/// One key's aggregate.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregateGroup {
    /// Each key element as Arrow displays it at the dataset type.
    pub key: Vec<Option<String>>,
    /// Lowercase hex of the canonical key tuple (ADR 016).
    pub key_hex: String,
    /// Rows of the key, exact, from the counts.
    pub rows: u64,
    /// Rows the aggregates below cover: every row when `residual_rows` is
    /// 0, else the rows of the answered row groups.
    pub answered_rows: u64,
    /// Rows among `answered_rows` whose value is null (ADR 014: counted,
    /// never summed or compared).
    pub nulls: u64,
    /// `count(value)`: the non-null values among `answered_rows`.
    pub count: u64,
    /// `sum(value)` over the answered rows, when asked; `null` value when
    /// every one is null.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sum: Option<AggValue>,
    /// `min(value)` over the answered rows, when asked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<AggValue>,
    /// `max(value)` over the answered rows, when asked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<AggValue>,
    /// Rows of the key in row groups still to read.
    pub residual_rows: u64,
    /// Row groups holding those rows.
    pub residual_row_groups: u64,
}

/// One aggregate value and how it was obtained.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggValue {
    /// The value as Arrow displays it at the dataset type (a sum of a
    /// decimal at its declared scale); `None` when every value is null.
    pub value: Option<String>,
    /// `Exact` for counts, decimal and integer sums, minimum and maximum;
    /// `Measured` by [`AGG_FLOAT_SUM_MODEL`] for a float sum, never exact.
    pub exactness: Exactness,
}

impl Engine {
    /// `fn(value) group by key where key in keys` over the keys in `keys`
    /// (B7), from the complete agg key on `value` whose key columns are
    /// among the key file's and which stores every function asked, beside
    /// the complete spec counting rows on exactly the key file's columns
    /// (`exact:<cols>,counts`). Per key: its exact rows, and the aggregates
    /// over every row group the counts prove an aggregate answers; the rest
    /// is the residual, the row groups to read (none when the agg key's
    /// columns are the key file's). Reads the manifest and index segments;
    /// no data file is opened, but the folder is checked for files changed
    /// since the build, as for every lookup. Writes nothing. Without the
    /// keys it needs, the lookup is refused naming the `index --key` that
    /// declares them.
    pub fn aggregate(
        &self,
        keys: &KeyFile,
        value: &str,
        functions: &[AggregateFn],
    ) -> Result<AggregateResult> {
        guard(|| self.aggregate_inner(keys, value, functions)).map_err(schema_hint)
    }

    fn aggregate_inner(
        &self,
        keys: &KeyFile,
        value: &str,
        functions: &[AggregateFn],
    ) -> Result<AggregateResult> {
        let pinned = self.pin_required("aggregate")?;
        self.refuse_stale(&pinned, "aggregate")?;
        let (specs, _) = pinned.specs();
        let schema = pinned.planner.schema();
        if schema.field_with_name(value).is_err() {
            return Err(Error::UnknownColumn {
                column: value.to_string(),
                root: self.root().to_string(),
                columns: schema.fields().iter().map(|f| f.name().clone()).collect(),
            });
        }
        let (run_keys, null_tuples) = keys.run_keys(schema)?;
        let columns = run_keys.columns().to_vec();
        let functions = AggregateFn::canonical(functions);
        if let Some(needs) = missing_spec(&specs, &columns, value, &functions) {
            return Err(Error::NotIndexed {
                root: self.root().to_string(),
                operation: "aggregate",
                needs: Some(needs),
            });
        }
        let tuples: Vec<Vec<u8>> = run_keys.tuples().map(<[u8]>::to_vec).collect();
        let key_set = Hash::of(&run_keys.to_payload()?);
        let answer = pinned.planner.aggregate(
            &specs,
            &AggLookup {
                columns: columns.clone(),
                keys: tuples,
                value: value.to_string(),
                functions: functions.clone(),
            },
        )?;
        let key_types: Vec<DataType> = pinned
            .planner
            .column_types(&columns)?
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        Ok(result(
            &answer,
            &key_types,
            &columns,
            value,
            &functions,
            (run_keys.len() as u64, null_tuples),
            key_set,
            pinned.sequence(),
        ))
    }
}

/// The declaration an aggregate lacks, as the spec `index --key` takes:
/// the agg key when none serves, else the counted key beside it; `None`
/// when both are complete.
fn missing_spec(
    specs: &[KeySpec],
    columns: &[String],
    value: &str,
    functions: &[AggregateFn],
) -> Option<String> {
    let agg = specs.iter().any(|s| {
        matches!(s, KeySpec::Agg { columns: c, value: v, functions: f }
            if v == value
                && c.iter().all(|x| columns.contains(x))
                && functions.iter().all(|g| f.contains(g)))
    });
    if !agg {
        return Some(
            KeySpec::Agg {
                columns: columns.to_vec(),
                value: value.to_string(),
                functions: AggregateFn::canonical(functions),
            }
            .identity(),
        );
    }
    let counted = specs.iter().any(|s| {
        matches!(s, KeySpec::Exact { .. } | KeySpec::Temporal { .. })
            && s.counts()
            && s.columns().len() == columns.len()
            && s.columns().iter().all(|c| columns.contains(c))
    });
    (!counted).then(|| format!("exact:{},counts", columns.join(",")))
}

/// The scale a sum of `dt` is rendered at.
fn sum_scale(dt: &DataType) -> Option<i8> {
    match dt {
        DataType::Dictionary(_, v) => sum_scale(v),
        DataType::Decimal128(_, s) => Some(*s),
        _ => None,
    }
}

fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

/// The typed result of one answer.
#[allow(clippy::too_many_arguments)]
fn result(
    answer: &Aggregated,
    key_types: &[DataType],
    columns: &[String],
    value: &str,
    functions: &[AggregateFn],
    (keys, null_tuples): (u64, u64),
    key_set: Hash,
    sequence: u64,
) -> AggregateResult {
    let value_type = std::slice::from_ref(&answer.value_type);
    let render = |bytes: &Option<Vec<u8>>| -> Option<String> {
        bytes
            .as_ref()
            .and_then(|b| render_tuple(b, value_type))
            .and_then(|v| v.into_iter().next().flatten())
    };
    let exact = || Exactness::Exact;
    let mut groups = Vec::with_capacity(answer.keys.len());
    let mut row_groups = roaring::RoaringBitmap::new();
    let mut rows = 0u64;
    for k in &answer.keys {
        let c = &k.cover;
        rows += c.rows;
        row_groups |= &c.row_groups;
        let cell = c.covered.clone().unwrap_or_else(AggCell::empty);
        let sum = functions.contains(&AggregateFn::Sum).then(|| {
            let (value, exactness) = match cell.sum {
                Some(s @ AggSum::Float { .. }) => (
                    Some(s.render(None)),
                    Exactness::Measured {
                        model: AGG_FLOAT_SUM_MODEL.to_string(),
                    },
                ),
                Some(s) if cell.values() > 0 => {
                    (Some(s.render(sum_scale(&answer.value_type))), exact())
                }
                // A float column whose answered rows are all null: no sum,
                // and a float sum is never exact.
                _ if matches!(
                    answer.value_type,
                    DataType::Float16 | DataType::Float32 | DataType::Float64
                ) =>
                {
                    (
                        None,
                        Exactness::Measured {
                            model: AGG_FLOAT_SUM_MODEL.to_string(),
                        },
                    )
                }
                _ => (None, exact()),
            };
            AggValue { value, exactness }
        });
        let min = functions.contains(&AggregateFn::Min).then(|| AggValue {
            value: render(&cell.min),
            exactness: exact(),
        });
        let max = functions.contains(&AggregateFn::Max).then(|| AggValue {
            value: render(&cell.max),
            exactness: exact(),
        });
        groups.push(AggregateGroup {
            key: render_tuple(&k.key, key_types).unwrap_or_default(),
            key_hex: hex(&k.key),
            rows: c.rows,
            answered_rows: cell.rows,
            nulls: cell.nulls,
            count: cell.values(),
            sum,
            min,
            max,
            residual_rows: c.residual_rows,
            residual_row_groups: c.residual.len(),
        });
    }
    AggregateResult {
        columns: columns.to_vec(),
        value: value.to_string(),
        functions: functions.iter().map(|f| f.as_str().to_string()).collect(),
        keys,
        null_tuples,
        key_set,
        sequence,
        answered_by: answer.agg_spec.identity(),
        counted_by: answer.counted_by.identity(),
        complete: answer.residual.row_groups_selected == 0,
        rows,
        row_groups: row_groups.len(),
        row_groups_total: answer.residual.row_groups_total,
        residual_row_groups: answer.residual.row_groups_selected,
        residual_bytes: answer.residual.bytes,
        residual_files: answer.residual.entries.len() as u64,
        groups,
    }
}
