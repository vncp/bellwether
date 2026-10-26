//! The check `fix` runs on an agg key's cells (B7) before it may certify
//! the key: every cell the key would store for the workload's keys equals
//! the cell the decoded rows make.
//!
//! The rows are read from the source, one row group at a time (ADR 018),
//! every live row group, so a cell for a row group that holds none of the
//! key's rows, and a row group whose cell is missing, are found as surely
//! as a wrong sum. A row's key is its canonical tuple over the agg key's
//! columns at the dataset types (ADR 016), skipped when its leading column
//! is null (ADR 014), exactly as an index key; its value is folded in row
//! order by [`AggCell::add_row`], so a float sum compares bit for bit. That
//! fold is the one piece shared with the writer, and the core crate's
//! property test checks it against a brute-force oracle; everything else
//! here (which rows belong to which key and row group, the decoding, the
//! decimal rescale) is computed from the rows alone.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use arrow::datatypes::{DataType, Schema};
use bellwether_core::agg::{functions, value_at, AggCell, AggSum};
use bellwether_core::encoding;
use bellwether_types::{
    AggCellCheck, AggCellDiff, Error, KeySpec, RowGroupId, RowGroupMeta, Source,
    MAX_MISMATCH_EXAMPLES,
};

use crate::error::Result;
use crate::render::render_tuple;
use crate::rowhash::hex;

/// Checks `stored`, the cells of the agg key `spec` per key and row group,
/// for `keys` (canonical tuples over `spec`'s columns at the dataset
/// types) against the rows of `row_groups` read through `source`: the
/// row groups listed must be every live row group, since a key's cell is
/// compared wherever the key has a row or a stored cell. Memory is one row
/// group's columns plus a cell per key per row group holding it, the same
/// as the cells checked. Returns the check and the rows the keys hold.
///
/// # Errors
/// A column missing from the schema or a row group, a value that does not
/// convert to the dataset type, or a source read failure.
#[allow(clippy::too_many_lines)] // the fold, then one comparison per cell
pub fn check_agg_cells<'a>(
    source: &dyn Source,
    schema: &Schema,
    row_groups: impl IntoIterator<Item = (RowGroupId, &'a RowGroupMeta)>,
    spec: &KeySpec,
    keys: &[Vec<u8>],
    stored: &BTreeMap<Vec<u8>, BTreeMap<u32, AggCell>>,
) -> Result<(AggCellCheck, u64)> {
    let KeySpec::Agg { value, .. } = spec else {
        return Err(Error::KeySpec(format!("{} is not an agg key", spec.identity())).into());
    };
    let fns = functions(spec);
    let type_of = |c: &str| -> Result<DataType> {
        Ok(schema
            .field_with_name(c)
            .map_err(|_| Error::KeySpec(encoding::not_in_schema(schema, c)))?
            .data_type()
            .clone())
    };
    let key_types = spec
        .columns()
        .iter()
        .map(|c| type_of(c))
        .collect::<Result<Vec<_>>>()?;
    let value_type = type_of(value)?;
    let mut columns = spec.columns().to_vec();
    columns.push(value.clone());
    let wanted: std::collections::BTreeSet<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let mut made: BTreeMap<Vec<u8>, BTreeMap<u32, AggCell>> = BTreeMap::new();
    let mut row_groups_read = 0u64;
    let (mut kbuf, mut vbuf) = (Vec::new(), Vec::new());
    for (id, meta) in row_groups {
        let batch = source.read_columns(meta, &columns)?;
        row_groups_read += 1;
        let arrays = columns
            .iter()
            .map(|c| {
                batch.column_by_name(c).cloned().ok_or_else(|| {
                    Error::Source(format!(
                        "{} row group {} has no column {c}",
                        meta.file.path, meta.index_in_file
                    ))
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let (key_arrays, values) = arrays.split_at(key_types.len());
        for row in 0..batch.num_rows() {
            kbuf.clear();
            // ADR 014: a null leading column is no key; a trailing null is
            // the null marker, as an index stores it.
            if !encoding::encode_array_value_as(
                key_arrays[0].as_ref(),
                row,
                &key_types[0],
                &mut kbuf,
            )? {
                continue;
            }
            for (a, t) in key_arrays.iter().zip(&key_types).skip(1) {
                encoding::encode_nullable_as(a.as_ref(), row, t, &mut kbuf)?;
            }
            if !wanted.contains(kbuf.as_slice()) {
                continue;
            }
            let v = value_at(values[0].as_ref(), row, &value_type, &mut vbuf)?;
            made.entry(kbuf.clone())
                .or_default()
                .entry(id.0)
                .or_insert_with(AggCell::empty)
                .add_row(v.then_some(vbuf.as_slice()), fns)?;
        }
    }
    let scale = match &value_type {
        DataType::Decimal128(_, s) => Some(*s),
        _ => None,
    };
    let render = |c: Option<&AggCell>| -> String {
        let Some(c) = c else {
            return "none".into();
        };
        let mut s = format!("rows {}, nulls {}", c.rows, c.nulls);
        if let Some(sum) = &c.sum {
            let _ = write!(s, ", sum {}", sum.render(scale));
            if let AggSum::Float { .. } = sum {
                s.push_str(" (inexact)");
            }
        }
        for (name, v) in [("min", &c.min), ("max", &c.max)] {
            if let Some(b) = v {
                let shown = render_tuple(b, std::slice::from_ref(&value_type))
                    .and_then(|mut v| v.pop().flatten())
                    .unwrap_or_else(|| hex(b));
                let _ = write!(s, ", {name} {shown}");
            }
        }
        s
    };
    let empty = BTreeMap::new();
    let (mut cells, mut differ, mut rows) = (0u64, 0u64, 0u64);
    let mut examples = Vec::new();
    for key in &wanted {
        let have = stored.get(*key).unwrap_or(&empty);
        let want = made.get(*key).unwrap_or(&empty);
        let mut rgs: Vec<u32> = have.keys().chain(want.keys()).copied().collect();
        rgs.sort_unstable();
        rgs.dedup();
        for rg in rgs {
            cells += 1;
            let (h, w) = (have.get(&rg), want.get(&rg));
            if let Some(w) = w {
                rows += w.rows;
            }
            if h == w {
                continue;
            }
            differ += 1;
            if examples.len() < MAX_MISMATCH_EXAMPLES {
                examples.push(AggCellDiff {
                    key: hex(key),
                    values: render_tuple(key, &key_types).unwrap_or_default(),
                    row_group: rg,
                    stored: render(h),
                    rows: render(w),
                });
            }
        }
    }
    Ok((
        AggCellCheck {
            keys: wanted.len() as u64,
            cells,
            row_groups_read,
            differ,
            examples,
        },
        rows,
    ))
}
