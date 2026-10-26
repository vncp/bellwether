//! One recorded run's key set ([`RunKeys`]) and its cache payload.
//!
//! A run's keys are the distinct, non-null key tuples it looked up, over
//! named columns. Each element is the canonical encoding (ADR 016) of the
//! value **converted to the dataset schema's type** for its column, so keys
//! recorded from a file whose column is `UInt32` probe an index over a
//! dataset that declares `Int64`. A value that does not convert exactly is an
//! error, never a rounded or skipped key.
//!
//! Nulls follow ADR 014: a probe tuple that contains a null matches nothing
//! (SQL three-valued logic), so it can contribute no row group to any plan.
//! Building a key set drops such tuples and counts them
//! ([`RunKeysBuilder::null_tuples`]); the count is recorded next to the run
//! in the workload manifest, so the drop is visible, not silent.
//!
//! # Payload (`cache/<blake3>.bin`)
//!
//! A key set is stored in the cache under the BLAKE3 of these bytes. All
//! integers are little-endian.
//!
//! ```text
//! magic "BWKEYSET" | payload_version u32 | header_len u32 | header JSON
//!     {"encoding_version", "columns", "key_types"}
//! tuple_count u64 | per tuple, per column: len u32 | canonical element bytes
//! ```
//!
//! Tuples are strictly increasing by their concatenated bytes (sorted, no
//! duplicates), every element is a well-formed, non-null canonical encoding,
//! and nothing follows the last tuple. A reader refuses anything else, and a
//! newer payload or encoding version is refused with
//! [`Error::UnsupportedVersion`] (ADR 017).

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Schema};
use bellwether_core::encoding::{self, Value};
use bellwether_core::segment::ColumnType;
use bellwether_types::format::ENCODING_VERSION;
use bellwether_types::{Error, Hash, Result};
use serde::{Deserialize, Serialize};

/// First eight bytes of every key-set payload.
pub const KEYSET_MAGIC: [u8; 8] = *b"BWKEYSET";
/// Version of the key-set payload layout this build writes and reads.
pub const KEYSET_VERSION: u32 = 1;

/// The JSON header of a key-set payload. Field order is part of the payload
/// bytes and therefore of the key set's hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct KeySetHeader {
    encoding_version: u32,
    columns: Vec<String>,
    key_types: Vec<ColumnType>,
}

/// One run's key set: distinct, non-null key tuples over `columns`, each
/// element canonical-encoded at the dataset type, sorted by the tuples'
/// concatenated bytes (which, because every element is self-delimiting and
/// memcomparable, is the tuples' typed order).
///
/// Memory (ADR 018): replay holds one run's key set at a time; this is it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunKeys {
    columns: Vec<String>,
    key_types: Vec<ColumnType>,
    /// Every tuple's elements, concatenated, tuples in order.
    data: Vec<u8>,
    /// End offset in `data` of element `j` of tuple `i`, at `i * width + j`.
    ends: Vec<usize>,
}

impl RunKeys {
    /// Key columns, in tuple order.
    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// The dataset type of each key column, as recorded (core's spelling).
    #[must_use]
    pub fn key_types(&self) -> &[ColumnType] {
        &self.key_types
    }

    /// Number of distinct key tuples.
    #[must_use]
    pub fn len(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.ends.len() / self.columns.len()
        }
    }

    /// Whether the run has no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn width(&self) -> usize {
        self.columns.len()
    }

    fn start_of(&self, flat: usize) -> usize {
        if flat == 0 {
            0
        } else {
            self.ends[flat - 1]
        }
    }

    /// Tuple `i`: the concatenation of its canonical elements.
    #[must_use]
    pub fn tuple(&self, i: usize) -> &[u8] {
        let w = self.width();
        &self.data[self.start_of(i * w)..self.ends[i * w + w - 1]]
    }

    /// Element `j` of tuple `i`.
    #[must_use]
    pub fn element(&self, i: usize, j: usize) -> &[u8] {
        let flat = i * self.width() + j;
        &self.data[self.start_of(flat)..self.ends[flat]]
    }

    /// The tuples in order.
    pub fn tuples(&self) -> impl Iterator<Item = &[u8]> + '_ {
        (0..self.len()).map(|i| self.tuple(i))
    }

    /// Probe keys for tuple `i` projected onto the key columns at `project`
    /// (indexes into [`RunKeys::columns`], in the order the index expects).
    /// A float-zero element expands to both the `+0.0` and the `-0.0`
    /// encoding (`docs/format.md`, statistics semantics), so a tuple yields
    /// one probe per combination, sorted and distinct.
    #[must_use]
    pub fn probes(&self, i: usize, project: &[usize]) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = vec![Vec::new()];
        for &j in project {
            let elem = self.element(i, j);
            match float_zero_twin(elem) {
                None => {
                    for p in &mut out {
                        p.extend_from_slice(elem);
                    }
                }
                Some(twin) => {
                    let mut next = Vec::with_capacity(out.len() * 2);
                    for p in out {
                        let mut a = p.clone();
                        a.extend_from_slice(elem);
                        let mut b = p;
                        b.extend_from_slice(&twin);
                        next.push(a);
                        next.push(b);
                    }
                    out = next;
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// The probes of every tuple for `project`, sorted and distinct.
    #[must_use]
    pub fn all_probes(&self, project: &[usize]) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = (0..self.len())
            .flat_map(|i| self.probes(i, project))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Indexes of `columns` among this run's key columns, or `None` if one
    /// is absent.
    #[must_use]
    pub fn positions(&self, columns: &[String]) -> Option<Vec<usize>> {
        columns
            .iter()
            .map(|c| self.columns.iter().position(|k| k == c))
            .collect()
    }

    /// The cache payload (module docs). Deterministic: equal key sets give
    /// equal bytes.
    pub fn to_payload(&self) -> Result<Vec<u8>> {
        let header = serde_json::to_vec(&KeySetHeader {
            encoding_version: ENCODING_VERSION,
            columns: self.columns.clone(),
            key_types: self.key_types.clone(),
        })?;
        let mut out = Vec::with_capacity(24 + header.len() + self.data.len() + 4 * self.ends.len());
        out.extend_from_slice(&KEYSET_MAGIC);
        out.extend_from_slice(&KEYSET_VERSION.to_le_bytes());
        out.extend_from_slice(&len_u32(header.len())?.to_le_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(&(self.len() as u64).to_le_bytes());
        for flat in 0..self.ends.len() {
            let elem = &self.data[self.start_of(flat)..self.ends[flat]];
            out.extend_from_slice(&len_u32(elem.len())?.to_le_bytes());
            out.extend_from_slice(elem);
        }
        Ok(out)
    }

    /// BLAKE3 of [`RunKeys::to_payload`]: the key set's cache name.
    pub fn hash(&self) -> Result<Hash> {
        Ok(Hash::of(&self.to_payload()?))
    }

    /// Parses and validates a payload (module docs).
    pub fn from_payload(bytes: &[u8]) -> Result<Self> {
        let mut c = Reader { bytes, pos: 0 };
        if c.take(8)? != KEYSET_MAGIC {
            return Err(corrupt("bad magic"));
        }
        let version = c.u32()?;
        if version > KEYSET_VERSION {
            return Err(Error::UnsupportedVersion {
                what: "key set",
                found: version,
                supported: KEYSET_VERSION,
            });
        }
        if version != KEYSET_VERSION {
            return Err(corrupt(&format!("unknown payload version {version}")));
        }
        let header_len = c.u32()? as usize;
        let header: KeySetHeader = serde_json::from_slice(c.take(header_len)?)?;
        if header.encoding_version > ENCODING_VERSION {
            return Err(Error::UnsupportedVersion {
                what: "encoding",
                found: header.encoding_version,
                supported: ENCODING_VERSION,
            });
        }
        if header.encoding_version != ENCODING_VERSION {
            return Err(corrupt(&format!(
                "unknown encoding version {}",
                header.encoding_version
            )));
        }
        if header.columns.is_empty() || header.columns.len() != header.key_types.len() {
            return Err(corrupt("columns and key types disagree"));
        }
        check_distinct_columns(&header.columns)?;
        let count = c.u64()?;
        let width = header.columns.len();
        // Every element takes at least five bytes (length and a tag), so a
        // count larger than the remaining bytes allow is corrupt; checking
        // first keeps a forged count from driving a huge allocation.
        let remaining = (bytes.len() - c.pos) as u64;
        if count.saturating_mul(width as u64).saturating_mul(5) > remaining {
            return Err(corrupt("tuple count exceeds the payload"));
        }
        let mut keys = RunKeys {
            columns: header.columns,
            key_types: header.key_types,
            data: Vec::with_capacity(bytes.len() - c.pos),
            ends: Vec::with_capacity(usize::try_from(count).unwrap_or(0) * width),
        };
        for i in 0..count {
            let start = keys.data.len();
            for _ in 0..width {
                let len = c.u32()? as usize;
                let elem = c.take(len)?;
                if encoding::tuple_has_null(elem) != Some(false) || !single_element(elem) {
                    return Err(corrupt(
                        "element is not one well-formed, non-null canonical value",
                    ));
                }
                keys.data.extend_from_slice(elem);
                keys.ends.push(keys.data.len());
            }
            if i > 0 {
                let prev = keys.tuple(usize::try_from(i - 1).unwrap_or(0));
                if prev >= &keys.data[start..] {
                    return Err(corrupt("tuples are not strictly increasing"));
                }
            }
        }
        if c.pos != bytes.len() {
            return Err(corrupt("trailing bytes"));
        }
        Ok(keys)
    }
}

/// Accumulates a run's key tuples from Arrow batches, then sorts and
/// deduplicates them ([`RunKeysBuilder::finish`]).
#[derive(Debug)]
pub struct RunKeysBuilder {
    columns: Vec<String>,
    /// The key columns as the batches name them: `columns`, except that an
    /// expression column is named as given while `columns` holds its
    /// normalized form.
    names: Vec<String>,
    declared: Vec<DataType>,
    key_types: Vec<ColumnType>,
    tuples: Vec<(Vec<u8>, Vec<usize>)>,
    null_tuples: u64,
    rows: u64,
}

impl RunKeysBuilder {
    /// A builder for key tuples over `columns`, encoded at the types the
    /// dataset schema declares for them. Every column must exist in the
    /// schema with a type that has a canonical encoding, and no column may
    /// repeat. One column may instead be an expression over dataset columns
    /// (`bellwether_core::expr`), alone: its keys are values of the
    /// expression, typed as it is, and the run records it normalized.
    pub fn new(columns: &[String], schema: &Schema) -> Result<Self> {
        if columns.is_empty() {
            return Err(Error::KeySpec(
                "a run's keys need at least one column".into(),
            ));
        }
        check_distinct_columns(columns)?;
        let mut declared = Vec::with_capacity(columns.len());
        let mut key_types = Vec::with_capacity(columns.len());
        let mut normalized = Vec::with_capacity(columns.len());
        for c in columns {
            let (name, dt) = match schema.field_with_name(c) {
                Ok(f) => (c.clone(), f.data_type().clone()),
                Err(_) if bellwether_core::expr::is_expression(c, schema) => {
                    if columns.len() > 1 {
                        return Err(Error::KeySpec(format!(
                            "a run keyed on an expression names that expression alone, not \
                             with other columns ({})",
                            columns.join(", ")
                        )));
                    }
                    let t = bellwether_core::expr::Typed::parse(c, schema)?;
                    (t.expr().to_string(), t.data_type().clone())
                }
                Err(_) => {
                    return Err(Error::KeySpec(format!(
                        "key {}",
                        encoding::not_in_schema(schema, c)
                    )))
                }
            };
            // A list (or map) column's run values are its elements (B4):
            // they encode at the element type.
            let dt = match bellwether_core::list::element_type(&dt) {
                Some(_) => bellwether_core::list::check_list_column(c, &dt)?,
                None => dt,
            };
            key_types.push(
                ColumnType::of(&dt)
                    .map_err(|e| Error::KeySpec(format!("key column {c:?}: {e}")))?,
            );
            declared.push(dt);
            normalized.push(name);
        }
        Ok(Self {
            columns: normalized,
            names: columns.to_vec(),
            declared,
            key_types,
            tuples: Vec::new(),
            null_tuples: 0,
            rows: 0,
        })
    }

    /// Adds every row of `batch`. The batch must hold exactly the key
    /// columns (any order, found by name); an extra or missing column is an
    /// error, because silently dropping one would change what the run looked
    /// up. Each value is converted to its dataset type; one that does not
    /// convert fails the whole batch and nothing from it is kept.
    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let schema = batch.schema();
        if schema.fields().len() != self.columns.len() {
            return Err(Error::KeySpec(format!(
                "a key batch has columns {:?}; the run's key columns are {:?}",
                schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
                self.columns
            )));
        }
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for ((c, n), dt) in self.columns.iter().zip(&self.names).zip(&self.declared) {
            let arr = batch
                .column_by_name(n)
                .or_else(|| batch.column_by_name(c))
                .ok_or_else(|| {
                    Error::KeySpec(format!("a key batch is missing key column {c:?}"))
                })?;
            encoding::check_convertible(arr.data_type(), dt)?;
            cols.push(ArrayRef::clone(arr));
        }
        let mut staged = Vec::with_capacity(batch.num_rows());
        let mut nulls = 0u64;
        for row in 0..batch.num_rows() {
            let mut buf = Vec::new();
            let mut ends = Vec::with_capacity(cols.len());
            let mut has_null = false;
            for (arr, dt) in cols.iter().zip(&self.declared) {
                if !encoding::encode_array_value_as(arr.as_ref(), row, dt, &mut buf)? {
                    has_null = true;
                    break;
                }
                ends.push(buf.len());
            }
            if has_null {
                nulls += 1;
            } else {
                staged.push((buf, ends));
            }
        }
        self.tuples.extend(staged);
        self.null_tuples += nulls;
        self.rows += batch.num_rows() as u64;
        Ok(())
    }

    /// Input rows whose tuple held a null and was dropped (ADR 014).
    #[must_use]
    pub fn null_tuples(&self) -> u64 {
        self.null_tuples
    }

    /// Input rows seen.
    #[must_use]
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// The distinct tuples, sorted.
    #[must_use]
    pub fn finish(mut self) -> RunKeys {
        self.tuples.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        self.tuples.dedup_by(|a, b| a.0 == b.0);
        let mut keys = RunKeys {
            columns: self.columns,
            key_types: self.key_types,
            data: Vec::with_capacity(self.tuples.iter().map(|t| t.0.len()).sum()),
            ends: Vec::with_capacity(self.tuples.len() * self.declared.len()),
        };
        for (bytes, ends) in self.tuples {
            let base = keys.data.len();
            keys.data.extend_from_slice(&bytes);
            keys.ends.extend(ends.into_iter().map(|e| base + e));
        }
        keys
    }
}

/// The other zero's encoding when `elem` encodes `+0.0` or `-0.0`.
fn float_zero_twin(elem: &[u8]) -> Option<Vec<u8>> {
    let pos = canonical_float(0.0);
    let neg = canonical_float(-0.0);
    if elem == pos.as_slice() {
        Some(neg)
    } else if elem == neg.as_slice() {
        Some(pos)
    } else {
        None
    }
}

fn canonical_float(v: f64) -> Vec<u8> {
    let mut out = Vec::with_capacity(9);
    // A Float64 value always encodes under its natural type.
    let _ = encoding::encode_value(&Value::Float(v), &mut out);
    out
}

/// Whether `elem` is exactly one canonical element (not a tuple of two).
fn single_element(elem: &[u8]) -> bool {
    // Re-encoding is not possible without a decoder, so check the one shape
    // that could hide a second element: a fixed-width tag followed by more
    // bytes than its width. Variable-width elements (strings, binary) end at
    // their terminator, which `tuple_has_null` already located.
    let fixed = match elem.first() {
        Some(0x01) => Some(2),
        Some(0x02..=0x04) => Some(9),
        Some(0x05) => match elem.get(1) {
            Some(0x10) => Some(18),
            Some(0x20) => Some(34),
            _ => None,
        },
        Some(0x08) => Some(11),
        _ => None,
    };
    if let Some(n) = fixed {
        return elem.len() == n;
    }
    // Strings and binary: the first unescaped terminator ends the element;
    // it must be the last two bytes.
    let mut j = 1;
    while j + 1 < elem.len() {
        if elem[j] == 0 {
            if elem[j + 1] == 0 {
                return j + 2 == elem.len();
            }
            j += 2;
        } else {
            j += 1;
        }
    }
    false
}

/// The dataset type a run key column takes: a dataset column's own, or,
/// for a column named by an expression (`bellwether_core::expr`), the
/// expression's value type.
pub(crate) fn declared_type(schema: &Schema, column: &str) -> Result<DataType> {
    if let Ok(f) = schema.field_with_name(column) {
        return Ok(f.data_type().clone());
    }
    if bellwether_core::expr::is_expression(column, schema) {
        return Ok(bellwether_core::expr::Typed::parse(column, schema)?
            .data_type()
            .clone());
    }
    Err(Error::KeySpec(format!(
        "key column {column:?} is not in the dataset"
    )))
}

pub(crate) fn check_distinct_columns(columns: &[String]) -> Result<()> {
    for (i, c) in columns.iter().enumerate() {
        if columns[..i].contains(c) {
            return Err(Error::KeySpec(format!("key column {c:?} appears twice")));
        }
    }
    Ok(())
}

fn len_u32(n: usize) -> Result<u32> {
    u32::try_from(n).map_err(|_| Error::Other(format!("key-set element of {n} bytes is too large")))
}

fn corrupt(what: &str) -> Error {
    Error::Other(format!("corrupt key set: {what}"))
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.bytes.len())
            .ok_or_else(|| corrupt("truncated"))?;
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray, UInt32Array};
    use arrow::datatypes::Field;

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("x", DataType::Float64, true),
        ])
    }

    fn cols(c: &[&str]) -> Vec<String> {
        c.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn keys_encode_at_the_dataset_type_sorted_distinct_without_nulls() {
        let s = schema();
        let mut b = RunKeysBuilder::new(&cols(&["id", "name"]), &s).unwrap();
        // A UInt32 file column under a dataset Int64: encoded with the Int tag.
        let batch = RecordBatch::try_from_iter(vec![
            (
                "name",
                Arc::new(StringArray::from(vec![
                    Some("b"),
                    Some("a"),
                    None,
                    Some("b"),
                ])) as ArrayRef,
            ),
            (
                "id",
                Arc::new(UInt32Array::from(vec![Some(2), Some(1), Some(3), Some(2)])) as ArrayRef,
            ),
        ])
        .unwrap();
        b.push_batch(&batch).unwrap();
        assert_eq!(b.null_tuples(), 1);
        let keys = b.finish();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys.columns(), cols(&["id", "name"]));
        assert_eq!(keys.element(0, 0)[0], 0x02, "dataset tag, not the file's");
        assert!(keys.tuple(0) < keys.tuple(1));
        let back = RunKeys::from_payload(&keys.to_payload().unwrap()).unwrap();
        assert_eq!(back, keys);
        assert_eq!(back.hash().unwrap(), keys.hash().unwrap());
    }

    #[test]
    fn a_batch_with_an_extra_or_missing_column_is_refused() {
        let s = schema();
        let mut b = RunKeysBuilder::new(&cols(&["id"]), &s).unwrap();
        let extra = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("name", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
        ])
        .unwrap();
        assert!(b.push_batch(&extra).is_err());
        let wrong = RecordBatch::try_from_iter(vec![(
            "name",
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
        )])
        .unwrap();
        assert!(b.push_batch(&wrong).is_err());
        assert!(RunKeysBuilder::new(&cols(&["nope"]), &s).is_err());
        assert!(RunKeysBuilder::new(&cols(&["id", "id"]), &s).is_err());
    }

    #[test]
    fn a_value_that_does_not_convert_fails_the_batch() {
        let s = Schema::new(vec![Field::new("id", DataType::Int8, false)]);
        let mut b = RunKeysBuilder::new(&cols(&["id"]), &s).unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1, 300])) as ArrayRef,
        )])
        .unwrap();
        assert!(b.push_batch(&batch).is_err());
        assert!(b.finish().is_empty(), "nothing from a failed batch is kept");
    }

    #[test]
    fn float_zero_probes_expand_to_both_encodings() {
        let s = schema();
        let mut b = RunKeysBuilder::new(&cols(&["x"]), &s).unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "x",
            Arc::new(Float64Array::from(vec![0.0, 1.5])) as ArrayRef,
        )])
        .unwrap();
        b.push_batch(&batch).unwrap();
        let keys = b.finish();
        let zero = keys
            .tuples()
            .position(|t| t == canonical_float(0.0).as_slice())
            .unwrap();
        let probes = keys.probes(zero, &[0]);
        assert_eq!(probes.len(), 2);
        assert!(probes.contains(&canonical_float(-0.0)));
        assert_eq!(keys.all_probes(&[0]).len(), 3);
    }

    #[test]
    fn corrupt_payloads_are_refused() {
        let s = schema();
        let mut b = RunKeysBuilder::new(&cols(&["id"]), &s).unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        b.push_batch(&batch).unwrap();
        let good = b.finish().to_payload().unwrap();
        // Every truncation fails.
        for n in 0..good.len() {
            assert!(
                RunKeys::from_payload(&good[..n]).is_err(),
                "truncated at {n}"
            );
        }
        let mut trailing = good.clone();
        trailing.push(0);
        assert!(RunKeys::from_payload(&trailing).is_err());
        // A newer payload version is a typed refusal.
        let mut newer = good.clone();
        newer[8] = 2;
        assert!(matches!(
            RunKeys::from_payload(&newer),
            Err(Error::UnsupportedVersion {
                what: "key set",
                ..
            })
        ));
        // Out-of-order tuples: swap the last two elements' payload bytes.
        let mut swapped = good.clone();
        let n = swapped.len();
        swapped.swap(n - 1, n - 14);
        assert!(RunKeys::from_payload(&swapped).is_err());
    }

    #[test]
    fn single_element_detects_concatenations() {
        let one = canonical_float(1.0);
        assert!(single_element(&one));
        let mut two = one.clone();
        two.extend_from_slice(&one);
        assert!(!single_element(&two));
        assert!(single_element(&[0x06, b'a', 0, 0]));
        assert!(!single_element(&[0x06, b'a', 0, 0, 0x06, 0, 0]));
        assert!(single_element(&[0x06, b'a', 0, 0xFF, b'b', 0, 0]));
    }
}
