//! List keys (B4, `KeySpec::List`): an inverted index over a list column.
//!
//! A list key over column `c` stores, per distinct **element** value of `c`
//! (its canonical encoding at the element's dataset type, ADR 016), the
//! row groups holding a row whose list contains that value. The segment is
//! the exact layout (`docs/format.md`, "List keys"): sorted element blocks,
//! one bloom per block, element to roaring bitmap over global `RowGroupId`.
//!
//! Which columns qualify: an Arrow `List<T>` or `LargeList<T>` whose item
//! type `T` has a canonical encoding, or a `Map<K, V>`, indexed by its keys
//! `K`. Nothing else is a list column: a fixed-size list, a list view, or a
//! list of a nested type is refused with the reason.
//!
//! Nulls (ADR 014): a null list, an empty list, and a null element are not
//! indexed. A row whose list is empty holds no element, so no membership
//! probe can match it, and a probe value that is null matches nothing (SQL
//! `array_contains(x, NULL)` is unknown, never true). So `contains_any` and
//! `contains_all` never need such a row, and the index is exact without it.
//!
//! Lookups:
//! - [`contains_any`]: the union, over the queried values, of each value's
//!   row groups;
//! - [`contains_all`]: the intersection, over the queried values, of each
//!   value's row groups. A row group that holds every value somewhere, in
//!   different rows, is kept: the plan is a superset, and the residual
//!   (`Predicate::ContainsAll`) rechecks membership per row.
//!
//! Both read each segment's block index, the blocks the values fall in, and
//! one bitmap per present value (ADR 018).

use std::collections::BTreeMap;
use std::ops::Range;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::DataType;
use bellwether_types::{Error, Result};
use roaring::RoaringBitmap;

use crate::encoding::{self, KeyColumns};
use crate::segment::{ReadAt, SegmentReader};

/// The element type of a list column of dataset type `dt`: the item type of
/// a `List` or `LargeList`, or the key type of a `Map`. `None` for any other
/// type.
#[must_use]
pub fn element_type(dt: &DataType) -> Option<&DataType> {
    match dt {
        DataType::List(f) | DataType::LargeList(f) => Some(f.data_type()),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) => fields.first().map(|f| f.data_type()),
            _ => None,
        },
        _ => None,
    }
}

/// The type a lookup value on a column of dataset type `dt` is encoded at:
/// a list column's element type ([`element_type`]), any other column's own
/// type.
#[must_use]
pub fn value_type(dt: &DataType) -> &DataType {
    element_type(dt).unwrap_or(dt)
}

/// The element type a list key over `column`, of dataset type `dt`, encodes
/// its elements at, or why the column cannot carry one, as a sentence that
/// says what to do.
pub fn check_list_column(column: &str, dt: &DataType) -> Result<DataType> {
    let Some(element) = element_type(dt) else {
        return Err(Error::KeySpec(format!(
            "list:{column} needs a list column (an Arrow List or LargeList, or a Map, indexed by \
             its keys), and {column:?} has type {dt}; key a flat column with exact:{column} \
             instead"
        )));
    };
    if let Some(kind) = encoding::nested_kind(element) {
        return Err(Error::KeySpec(format!(
            "list:{column} indexes the elements of {column:?}, and its elements are a nested \
             {kind}, which has no canonical encoding; a list key needs elements of a flat type \
             (integer, float, decimal, string, binary, date, time, or timestamp)"
        )));
    }
    encoding::check_supported(element)
        .map_err(|e| Error::KeySpec(format!("list:{column}: its elements: {e}")))?;
    Ok(element.clone())
}

/// The elements of one batch's list column, row by row: each row's range
/// into one flat array of element values.
#[derive(Debug, Clone)]
pub struct ListElements {
    rows: Vec<Option<Range<usize>>>,
    values: ArrayRef,
}

impl ListElements {
    /// The elements of `array`, a `List`, `LargeList`, or `Map` array (its
    /// keys). A null row has no range; an empty list has an empty one.
    pub fn new(array: &ArrayRef) -> Result<Self> {
        fn ranges<O: arrow::array::OffsetSizeTrait>(
            offsets: &[O],
            nulls: &dyn Array,
        ) -> Result<Vec<Option<Range<usize>>>> {
            (0..nulls.len())
                .map(|row| {
                    if nulls.is_null(row) {
                        return Ok(None);
                    }
                    let (Some(start), Some(end)) = (offsets.get(row), offsets.get(row + 1)) else {
                        return Err(Error::Other("list offsets shorter than the array".into()));
                    };
                    Ok(Some(start.as_usize()..end.as_usize()))
                })
                .collect()
        }
        match array.data_type() {
            DataType::List(_) => {
                let a = array.as_list::<i32>();
                Ok(Self {
                    rows: ranges(a.value_offsets(), a)?,
                    values: ArrayRef::clone(a.values()),
                })
            }
            DataType::LargeList(_) => {
                let a = array.as_list::<i64>();
                Ok(Self {
                    rows: ranges(a.value_offsets(), a)?,
                    values: ArrayRef::clone(a.values()),
                })
            }
            DataType::Map(_, _) => {
                let a = array.as_map();
                Ok(Self {
                    rows: ranges(a.value_offsets(), a)?,
                    values: ArrayRef::clone(a.keys()),
                })
            }
            other => Err(Error::KeySpec(format!(
                "a list key reads a List, LargeList, or Map column; this one has type {other}"
            ))),
        }
    }

    /// Number of rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether there are no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The flat element values every row's range points into.
    #[must_use]
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    /// Row `row`'s range into [`ListElements::values`]; `None` for a null
    /// list.
    #[must_use]
    pub fn row(&self, row: usize) -> Option<Range<usize>> {
        self.rows.get(row).cloned().flatten()
    }

    /// An encoder of the element values at the dataset element type
    /// `declared`; refuses an element type that does not convert to it.
    pub fn encoder(&self, declared: &DataType) -> Result<KeyColumns> {
        KeyColumns::new(
            std::slice::from_ref(&self.values),
            std::slice::from_ref(declared),
        )
    }

    /// Calls `f` with the canonical encoding of every non-null element of
    /// every non-null row, at the dataset element type `declared`, with its
    /// row. Duplicates within a row are passed as often as they occur. All
    /// conversions are checked before `f` is first called, so on error `f`
    /// has seen nothing.
    pub fn for_each_encoded(
        &self,
        declared: &DataType,
        mut f: impl FnMut(usize, &[u8]),
    ) -> Result<()> {
        let enc = self.encoder(declared)?;
        let mut staged: Vec<(usize, Vec<u8>)> = Vec::new();
        let mut buf = Vec::new();
        for (row, range) in self.rows.iter().enumerate() {
            let Some(range) = range else { continue };
            for i in range.clone() {
                if enc.encode_into(i, &mut buf)? {
                    staged.push((row, buf.clone()));
                }
            }
        }
        for (row, key) in &staged {
            f(*row, key);
        }
        Ok(())
    }
}

/// Row `row` of `array` (a `List`, `LargeList`, or `Map` array) as its own
/// element values (a map's keys), or `None` for a null row. O(1): a slice
/// of the child array, for testing one row's membership (a residual).
pub fn row_elements(array: &dyn Array, row: usize) -> Result<Option<ArrayRef>> {
    if row >= array.len() {
        return Err(Error::Other(format!(
            "row {row} of a {}-row list column",
            array.len()
        )));
    }
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(match array.data_type() {
        DataType::List(_) => array.as_list::<i32>().value(row),
        DataType::LargeList(_) => array.as_list::<i64>().value(row),
        DataType::Map(_, _) => ArrayRef::clone(array.as_map().value(row).column(0)),
        other => {
            return Err(Error::KeySpec(format!(
                "a membership test reads a List, LargeList, or Map column; this one has type \
                 {other}"
            )))
        }
    }))
}

/// The canonical encodings, at the dataset element type `element`, of the
/// non-null elements of row `row` of list column `array`, in order and with
/// duplicates: what a residual membership test compares. A null list and
/// an empty list give none (ADR 014).
pub fn row_element_keys(array: &dyn Array, row: usize, element: &DataType) -> Result<Vec<Vec<u8>>> {
    let Some(items) = row_elements(array, row)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(items.len());
    for i in 0..items.len() {
        let mut buf = Vec::new();
        if encoding::encode_array_value_as(items.as_ref(), i, element, &mut buf)? {
            out.push(buf);
        }
    }
    Ok(out)
}

/// Per queried value, the row groups of `readers` (every segment of one
/// list key) holding it: the union of its bitmaps over the segments. A
/// value no segment holds maps to an empty bitmap. Each value is one
/// canonical encoding.
pub fn element_row_groups<R: ReadAt, K: AsRef<[u8]>>(
    readers: &[SegmentReader<R>],
    values: &[K],
) -> Result<BTreeMap<Vec<u8>, RoaringBitmap>> {
    let mut out: BTreeMap<Vec<u8>, RoaringBitmap> = values
        .iter()
        .map(|v| (v.as_ref().to_vec(), RoaringBitmap::new()))
        .collect();
    for r in readers {
        check_list(r)?;
        for (k, v) in r.get(values)? {
            if let Some(acc) = out.get_mut(&k) {
                *acc |= &v.bitmap;
            }
        }
    }
    Ok(out)
}

/// The row groups that may hold a row whose list contains at least one of
/// `values` (canonical encodings at the element type; list both encodings
/// of a float zero), over every segment of one list key. No value, no row
/// group.
pub fn contains_any<R: ReadAt, K: AsRef<[u8]>>(
    readers: &[SegmentReader<R>],
    values: &[K],
) -> Result<RoaringBitmap> {
    Ok(element_row_groups(readers, values)?
        .into_values()
        .fold(RoaringBitmap::new(), |u, b| u | b))
}

/// The row groups that may hold a row whose list contains every queried
/// value, over every segment of one list key. Each group in `groups` is
/// one queried value: one canonical encoding, or both encodings of a float
/// zero, any of which counts. A row group is kept when it holds every
/// value (possibly in different rows; the residual rechecks per row).
/// No group at all is refused: "contains all of nothing" holds for every
/// row, null and empty lists included, which the index does not record.
pub fn contains_all<R: ReadAt, K: AsRef<[u8]>>(
    readers: &[SegmentReader<R>],
    groups: &[Vec<K>],
) -> Result<RoaringBitmap> {
    if groups.is_empty() {
        return Err(Error::KeySpec(
            "contains_all needs at least one value; with none, every row matches, null and empty \
             lists included, and the list index does not record those rows"
                .into(),
        ));
    }
    let flat: Vec<&[u8]> = groups.iter().flatten().map(AsRef::as_ref).collect();
    let per = element_row_groups(readers, &flat)?;
    let mut acc: Option<RoaringBitmap> = None;
    for g in groups {
        let any = g
            .iter()
            .filter_map(|v| per.get(v.as_ref()))
            .fold(RoaringBitmap::new(), |u, b| u | b);
        acc = Some(match acc {
            Some(a) => a & any,
            None => any,
        });
    }
    Ok(acc.unwrap_or_default())
}

/// The distinct element values over every segment of one list key, by SQL
/// equality: each segment's keys, merged in order so an element held by
/// several segments counts once, and the two encodings of a float zero
/// (`+0.0`, `-0.0`, which compare equal) counted as one value. Exact. Holds
/// one block of keys per segment (ADR 018).
pub fn distinct_elements<R: ReadAt>(readers: &[SegmentReader<R>]) -> Result<u64> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    struct Cursor {
        block: usize,
        keys: std::vec::IntoIter<Vec<u8>>,
    }
    let mut cursors: Vec<Cursor> = Vec::with_capacity(readers.len());
    // (next key, segment), smallest first
    let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
    let next = |r: &SegmentReader<R>, c: &mut Cursor| -> Result<Option<Vec<u8>>> {
        loop {
            if let Some(k) = c.keys.next() {
                return Ok(Some(k));
            }
            match r.block_keys(c.block)? {
                Some(keys) => {
                    c.block += 1;
                    c.keys = keys.into_iter();
                }
                None => return Ok(None),
            }
        }
    };
    for (i, r) in readers.iter().enumerate() {
        check_list(r)?;
        let mut c = Cursor {
            block: 0,
            keys: Vec::new().into_iter(),
        };
        if let Some(k) = next(r, &mut c)? {
            heap.push(Reverse((k, i)));
        }
        cursors.push(c);
    }
    let zero = |v: f64| {
        let mut out = Vec::with_capacity(9);
        // A float encodes under the one canonical float tag; this cannot fail.
        let _ = encoding::encode_value_as(&encoding::Value::Float(v), &DataType::Float64, &mut out);
        out
    };
    let zeros = [zero(0.0), zero(-0.0)];
    let mut zero_seen = false;
    let mut distinct = 0u64;
    let mut last: Option<Vec<u8>> = None;
    while let Some(Reverse((k, i))) = heap.pop() {
        if last.as_ref() != Some(&k) {
            if zeros.contains(&k) {
                if !zero_seen {
                    distinct += 1;
                }
                zero_seen = true;
            } else {
                distinct += 1;
            }
            last = Some(k);
        }
        if let Some(n) = next(&readers[i], &mut cursors[i])? {
            heap.push(Reverse((n, i)));
        }
    }
    Ok(distinct)
}

fn check_list<R: ReadAt>(r: &SegmentReader<R>) -> Result<()> {
    match r.key_spec() {
        bellwether_types::KeySpec::List { .. } => Ok(()),
        other => Err(Error::KeySpec(format!(
            "a membership lookup reads list key segments; this segment indexes {}",
            other.name()
        ))),
    }
}
