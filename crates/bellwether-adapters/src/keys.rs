//! A keys file in the query: the engine semi-joins the planned rows
//! against a Parquet file of the queried keys instead of an inline list.
//! The adapter reads the file once, encodes every key at the dataset types
//! exactly as the planner did, and refuses it unless its key set is the
//! residual's `In` list (equality as the engine compares, see
//! [`KeyEquality`]), so the semi-join keeps exactly the rows the list
//! would.

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;

use arrow::datatypes::DataType;
use bellwether_core::encoding::{tuple_elements, tuple_has_null, KeyColumns};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;

use crate::residual::{value_type, Bound, Cond, Residual, Stage};
use crate::{AdapterError, Result};

/// How an engine compares key values in a join.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyEquality {
    /// Canonical bytes (DataFusion after NaN normalization: `-0.0` and
    /// `+0.0` differ).
    Canonical,
    /// SQL equality (DuckDB, Spark): `-0.0 = +0.0`.
    Sql,
}

/// The canonical encoding of float `+0.0` and `-0.0`.
const POS_ZERO: [u8; 9] = [0x04, 0x80, 0, 0, 0, 0, 0, 0, 0];
const NEG_ZERO: [u8; 9] = [0x04, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];

/// `key` with every `-0.0` element written as `+0.0`: one representative of
/// the SQL equality class.
pub(crate) fn sql_class(key: &[u8]) -> Vec<u8> {
    let Some(spans) = tuple_elements(key) else {
        return key.to_vec();
    };
    let mut out = Vec::with_capacity(key.len());
    for sp in spans {
        let e = &key[sp];
        if e == NEG_ZERO {
            out.extend_from_slice(&POS_ZERO);
        } else {
            out.extend_from_slice(e);
        }
    }
    out
}

/// The number of float zero elements in a canonical tuple.
fn zeros(key: &[u8]) -> u32 {
    tuple_elements(key).map_or(0, |spans| {
        let n = spans
            .into_iter()
            .filter(|sp| key[sp.clone()] == POS_ZERO || key[sp.clone()] == NEG_ZERO)
            .count();
        u32::try_from(n).unwrap_or(u32::MAX)
    })
}

/// Whether a list of canonical tuples means the same set under SQL
/// equality (`-0.0 = +0.0`) as under canonical bytes: every tuple's whole
/// zero class (each float zero in both signs) is listed. The planner lists
/// a zero key in both encodings, so its plans pass; a list naming one sign
/// alone would match the other sign in a SQL engine, and is refused.
pub(crate) fn complete_zero_classes<'a>(raw: impl IntoIterator<Item = &'a Vec<u8>>) -> bool {
    let mut classes: std::collections::BTreeMap<Vec<u8>, (u32, BTreeSet<&'a Vec<u8>>)> =
        std::collections::BTreeMap::new();
    for k in raw {
        if tuple_has_null(k) != Some(false) {
            continue;
        }
        let e = classes.entry(sql_class(k)).or_insert_with(|| (zeros(k), BTreeSet::new()));
        e.1.insert(k);
    }
    classes
        .values()
        .all(|(z, members)| 1usize.checked_shl(*z).is_some_and(|n| members.len() == n))
}

/// The refusal for an incomplete zero class in `what` under `engine`.
pub(crate) fn zero_refusal(engine: &'static str, what: &str) -> AdapterError {
    AdapterError::Unsupported {
        engine,
        what: what.to_string(),
        reason: "lists a float zero in one sign only, and the engine compares -0.0 equal to \
                 +0.0, so it would also match the other sign"
            .into(),
        next: "emit for DataFusion, which compares them as the plan does".into(),
    }
}

/// The bounds of a `NotOlderThan` (`what` names it in refusals), one per
/// SQL equality class (a float zero key's two encodings are one class and
/// must share one bound), keys holding a null dropped (they match no row).
pub(crate) fn sql_bounds<'a>(
    bounds: &'a [Bound],
    engine: &'static str,
    what: &str,
) -> Result<Vec<&'a Bound>> {
    let listed: Vec<&Vec<u8>> = bounds.iter().map(|b| &b.raw_key).collect();
    if !complete_zero_classes(listed.iter().copied()) {
        return Err(zero_refusal(engine, what));
    }
    let mut by_class: std::collections::BTreeMap<Vec<u8>, &Bound> =
        std::collections::BTreeMap::new();
    for b in bounds {
        if b.key.iter().any(Option::is_none) {
            continue;
        }
        let class = sql_class(&b.raw_key);
        if let Some(prev) = by_class.get(&class) {
            if prev.ts != b.ts {
                return Err(AdapterError::Unsupported {
                    engine,
                    what: what.to_string(),
                    reason: "gives -0.0 and +0.0 of one key different bounds, and the engine \
                             compares them equal"
                        .into(),
                    next: "emit for DataFusion, which keeps the two zeros apart as the plan does"
                        .into(),
                });
            }
        } else {
            by_class.insert(class, b);
        }
    }
    Ok(by_class.into_values().collect())
}

/// The residual's one `In` stage: its columns, types, and canonical rows.
fn the_in(residual: &Residual, path: &str) -> Result<(Vec<String>, Vec<DataType>, Vec<Vec<u8>>)> {
    let ins: Vec<&Cond> = residual
        .stages
        .iter()
        .filter_map(|s| match s {
            Stage::Filter(c @ Cond::In { .. }) => Some(c),
            _ => None,
        })
        .collect();
    match ins.as_slice() {
        [Cond::In {
            columns,
            types,
            raw,
            ..
        }] => Ok((columns.clone(), types.clone(), raw.clone())),
        [] => Err(AdapterError::KeysFile {
            path: path.to_string(),
            reason: "the residual has no key list (In) for it to replace; emit without the keys \
                     file"
                .into(),
        }),
        _ => Err(AdapterError::KeysFile {
            path: path.to_string(),
            reason: "the residual has several key lists (In); emit without the keys file".into(),
        }),
    }
}

/// Checks that the Parquet file at `path` holds exactly the residual's `In`
/// keys under `equality`, with each key column at its dataset type, and
/// returns the key columns the engine joins on.
pub(crate) fn check(path: &Path, residual: &Residual, equality: KeyEquality) -> Result<Vec<String>> {
    let shown = path.display().to_string();
    let bad = |reason: String| AdapterError::KeysFile {
        path: shown.clone(),
        reason,
    };
    let (columns, types, raw) = the_in(residual, &shown)?;
    let file = File::open(path).map_err(|e| bad(format!("it cannot be opened ({e})")))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| bad(format!("it is not a readable Parquet file ({e})")))?;
    let file_schema = builder.schema().clone();
    let mut indices = Vec::with_capacity(columns.len());
    for (c, t) in columns.iter().zip(&types) {
        let (i, f) = file_schema.column_with_name(c).ok_or_else(|| {
            bad(format!(
                "it has no column {c:?}; its key columns must be named as the lookup's ({})",
                columns.join(", ")
            ))
        })?;
        if value_type(f.data_type()) != value_type(t) {
            return Err(bad(format!(
                "column {c:?} is {}, not the dataset type {t}; the engine would compare it \
                 after its own cast, so write the keys at the dataset type",
                f.data_type()
            )));
        }
        indices.push(i);
    }
    let mask = ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|e| bad(e.to_string()))?;
    let mut got = BTreeSet::new();
    for batch in reader {
        let batch = batch.map_err(|e| bad(e.to_string()))?;
        let arrays = columns
            .iter()
            .map(|c| {
                batch
                    .column_by_name(c)
                    .cloned()
                    .ok_or_else(|| bad(format!("column {c:?} is missing from a batch")))
            })
            .collect::<Result<Vec<_>>>()?;
        let keys = KeyColumns::new(&arrays, &types).map_err(|e| bad(e.to_string()))?;
        let mut buf = Vec::new();
        for row in 0..keys.len() {
            if keys.encode_into(row, &mut buf).map_err(|e| bad(e.to_string()))?
                && tuple_has_null(&buf) == Some(false)
            {
                got.insert(match equality {
                    KeyEquality::Canonical => buf.clone(),
                    KeyEquality::Sql => sql_class(&buf),
                });
            }
        }
    }
    let want: BTreeSet<Vec<u8>> = raw
        .iter()
        .filter(|k| tuple_has_null(k) == Some(false))
        .map(|k| match equality {
            KeyEquality::Canonical => k.clone(),
            KeyEquality::Sql => sql_class(k),
        })
        .collect();
    if got != want {
        let extra = got.difference(&want).count();
        let missing = want.difference(&got).count();
        return Err(bad(format!(
            "its {} distinct non-null keys are not the residual's {} (it has {extra} the list \
             lacks and lacks {missing}); a latest or asof lookup lists only the keys with a \
             version, and a float zero is listed as both -0.0 and +0.0, so pass a file of exactly \
             the listed keys or emit with the inline list",
            got.len(),
            want.len()
        )));
    }
    Ok(columns)
}
