//! Join keys (B3): the keys two datasets share on one column pair.
//!
//! A join key `join:<left>.<cols>=<right>.<cols>` is declared on the left
//! dataset and stored in its index as segments in the `keyed_join` layout
//! (`docs/format.md`, "Join keys"): per key value present on both sides,
//! the left value record (`bitmap_len u32 | bitmap | [counts]`, row groups
//! in the left dataset's id space) and then the right one (the right
//! dataset's id space). A key present on one side only is not stored; the
//! index ends with the number of such keys per side. The header pins what
//! the pair was built from (ADR 020): the right dataset's manifest by
//! sequence and content hash, and the left's manifest sequence and live
//! row-group set.
//!
//! [`build_pair`] builds it from both sides' exact segments on the join
//! columns by a merge join over their sorted key streams, each side a k-way
//! merge of its segments holding one block per segment at a time, and
//! splits the shared keys by key range into segments capped by
//! [`PairCaps`] (ADR 018); only the live row groups of each side count.
//! The same function builds the transient pair index `why` prices with, so
//! predicted equals planned by construction.
//!
//! SQL semantics, ADR 014: a tuple with a null element equals nothing, so
//! a stored exact tuple with a trailing null marker is never paired and is
//! counted as one-sided. Floats are refused as join columns: `-0.0` and
//! `0.0` are equal in SQL and encode differently (ADR 016), and engines
//! disagree on whether `NaN` equals itself, so any pairing would be a
//! guess. Two columns pair only when their canonical encodings compare
//! (integers of any width with integers of the same signedness, strings
//! with strings, decimals at one width and scale, times in one unit).

use std::collections::BTreeMap;

use bellwether_types::{Error, Hash, KeySpec, Result};
use roaring::RoaringBitmap;

use crate::encoding::tuple_has_null;
use crate::merge::MergedKeys;
use crate::segment::{
    ColumnType, Cursor, EncodedSegment, Index, KeyValue, KeyedEmitter, Layout, ReadAt,
    RecordedUnit, SegmentHeader, SegmentReader, WriterOptions,
};

/// One shared key's value: its rows' row groups on each side.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JoinValue {
    /// Left row groups (left dataset ids), with per-row-group rows when the
    /// key stores counts.
    pub left: KeyValue,
    /// Right row groups (right dataset ids), likewise.
    pub right: KeyValue,
}

/// One side of a pair build: the exact segments on its join columns and
/// the side's live row groups.
pub struct Side<'a, R> {
    /// Every segment of the side's complete exact key on the join columns.
    pub segments: &'a [SegmentReader<R>],
    /// The side's live row groups; ids outside it (retired by drift, ADR
    /// 013) are dropped from every bitmap, and a key held only there is
    /// absent from the side.
    pub live: &'a RoaringBitmap,
    /// The side's join column types, in column order, when known (the
    /// segment header records them as `key_types` / `right_key_types`).
    pub types: Option<Vec<ColumnType>>,
}

/// The overlap of a join key's two sides, exact, from the pair index.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OverlapStats {
    /// Keys on both sides, `|L ∩ R|`.
    pub both: u64,
    /// Keys on the left only, `|L \ R|` (a tuple with a null element is
    /// one-sided: it equals nothing).
    pub left_only: u64,
    /// Keys on the right only, `|R \ L|`.
    pub right_only: u64,
    /// Most right row groups one shared key is in.
    pub max_right_row_groups: u64,
    /// Right row groups summed over shared keys (their mean is this over
    /// `both`).
    pub sum_right_row_groups: u64,
    /// Most left row groups one shared key is in.
    pub max_left_row_groups: u64,
    /// Left row groups summed over shared keys.
    pub sum_left_row_groups: u64,
    /// With counts: rows of shared keys per side, and the rows of the inner
    /// join on the key (per shared key, left rows times right rows).
    pub rows: Option<PairRows>,
}

/// Exact row counts of a counted join key's shared keys.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PairRows {
    /// Left rows whose key the right holds: what `left ⋉ right` keeps.
    pub left: u64,
    /// Right rows whose key the left holds: what `right ⋉ left` keeps.
    pub right: u64,
    /// Rows of `left ⋈ right` on the key.
    pub join: u64,
}

/// Checks that one join column's type can pair at all (a float cannot;
/// see the module docs).
pub fn check_column_type(column: &str, t: &ColumnType) -> Result<()> {
    domain(column, t).map(|_| ())
}

/// The canonical encoding's value domain of a join column type: two
/// columns pair only when their domains are equal, so that equal SQL
/// values have equal bytes. A float is refused.
fn domain(column: &str, t: &ColumnType) -> Result<String> {
    let unit = |u: &RecordedUnit| match u {
        RecordedUnit::Second => "s",
        RecordedUnit::Millisecond => "ms",
        RecordedUnit::Microsecond => "us",
        RecordedUnit::Nanosecond => "ns",
    };
    Ok(match t {
        ColumnType::Boolean => "boolean".into(),
        ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 | ColumnType::Int64 => {
            "signed integer".into()
        }
        ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64 => {
            "unsigned integer".into()
        }
        ColumnType::Float16 | ColumnType::Float32 | ColumnType::Float64 => {
            return Err(Error::KeySpec(format!(
                "join column {column:?} is a float ({t}): -0.0 and 0.0 are equal in SQL but \
                 encode differently, and engines disagree on whether NaN equals NaN, so a join \
                 key on it would guess; join on an integer, decimal, string, date, or time \
                 column (cast the float to a decimal in both datasets)"
            )))
        }
        ColumnType::Decimal128 { scale, .. } => format!("decimal128 at scale {scale}"),
        ColumnType::Decimal256 { scale, .. } => format!("decimal256 at scale {scale}"),
        ColumnType::Utf8 | ColumnType::LargeUtf8 | ColumnType::Utf8View => "string".into(),
        ColumnType::Binary
        | ColumnType::LargeBinary
        | ColumnType::BinaryView
        | ColumnType::FixedSizeBinary { .. } => "binary".into(),
        ColumnType::Date32 => "date32".into(),
        ColumnType::Date64 => "date64".into(),
        ColumnType::Time32 { unit: u } | ColumnType::Time64 { unit: u } => {
            format!("time in {}", unit(u))
        }
        ColumnType::Timestamp { unit: u, .. } => format!("timestamp in {}", unit(u)),
    })
}

/// Checks that the join key's left and right column types pair: each left
/// column's canonical encoding compares with its right column's (see the
/// module docs). Refused, naming both types, otherwise.
pub fn check_comparable(spec: &KeySpec, left: &[ColumnType], right: &[ColumnType]) -> Result<()> {
    let KeySpec::Join {
        columns,
        right_columns,
        right: right_name,
        dataset,
        ..
    } = spec
    else {
        return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
    };
    if left.len() != columns.len() || right.len() != right_columns.len() {
        return Err(Error::KeySpec(format!(
            "{}: one type per join column is needed on each side",
            spec.name()
        )));
    }
    for (i, (l, r)) in left.iter().zip(right).enumerate() {
        let (dl, dr) = (domain(&columns[i], l)?, domain(&right_columns[i], r)?);
        if dl != dr {
            return Err(Error::KeySpec(format!(
                "{}: {dataset}.{} is {l} and {right_name}.{} is {r}, whose values never encode \
                 alike ({dl} against {dr}), so no key would pair; join columns of one kind (cast \
                 one side so both are {dl} or both {dr})",
                spec.name(),
                columns[i],
                right_columns[i]
            )));
        }
    }
    Ok(())
}

/// Checks a join key's own rules: both dataset names are one folder name
/// each (letters, digits, `_`, `-`), and the sides name as many columns,
/// at least one each, none twice. Applies to every join key, declared now
/// or read from an index.
pub fn check_spec(spec: &KeySpec) -> Result<()> {
    let KeySpec::Join {
        dataset,
        columns,
        right,
        right_columns,
        ..
    } = spec
    else {
        return Ok(());
    };
    for name in [dataset, right] {
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(Error::KeySpec(format!(
                "join key dataset name {name:?} is not a folder name of letters, digits, '_' or \
                 '-': the left dataset is the data root's folder and the right one the folder \
                 of that name beside it"
            )));
        }
    }
    if columns.is_empty() || columns.len() != right_columns.len() {
        return Err(Error::KeySpec(format!(
            "{} pairs {} left column(s) with {} right column(s); a join key pairs one or more \
             columns on each side, one for one",
            spec.name(),
            columns.len(),
            right_columns.len()
        )));
    }
    for cols in [columns, right_columns] {
        let mut seen = std::collections::BTreeSet::new();
        if let Some(c) = cols.iter().find(|c| !seen.insert(c.as_str())) {
            return Err(Error::KeySpec(format!(
                "{} lists column {c:?} twice on one side",
                spec.name()
            )));
        }
    }
    Ok(())
}

/// The exact spec on `columns` (with counts when `counts`) a join key's
/// side is built from: the identity a side's index must hold, complete.
#[must_use]
pub fn side_spec(columns: &[String], counts: bool) -> KeySpec {
    KeySpec::Exact {
        columns: columns.to_vec(),
        counts,
        bounds: None,
    }
}

/// `value` restricted to `live`: its bitmap intersected, its counts kept
/// for the row groups that remain. `None` when none remains.
fn live_only(value: KeyValue, live: &RoaringBitmap) -> Option<KeyValue> {
    if value.bitmap.is_subset(live) {
        return (!value.bitmap.is_empty()).then_some(value);
    }
    let counts = value.counts.as_ref().map(|c| {
        value
            .bitmap
            .iter()
            .zip(c)
            .filter(|(rg, _)| live.contains(*rg))
            .map(|(_, n)| *n)
            .collect::<Vec<u64>>()
    });
    let bitmap = &value.bitmap & live;
    // A join pairs row-group sets (and counts): an exact key's per-key
    // bounds are not part of a join value.
    (!bitmap.is_empty()).then_some(KeyValue {
        bitmap,
        chain: value.chain,
        counts,
        bounds: None,
        triples: Vec::new(),
        agg: None,
    })
}

/// A side's merged key stream with retired row groups dropped: the next
/// key the side holds in a live row group.
struct LiveKeys<'a, R> {
    keys: MergedKeys<'a, R>,
    live: &'a RoaringBitmap,
}

impl<R: ReadAt> LiveKeys<'_, R> {
    fn next(&mut self) -> Result<Option<(Vec<u8>, KeyValue)>> {
        while let Some((k, v)) = self.keys.next_key()? {
            if let Some(v) = live_only(v, self.live) {
                return Ok(Some((k, v)));
            }
        }
        Ok(None)
    }
}

/// Checks that `segments` are a side's exact segments on `columns`, with
/// counts when the join key stores them.
fn check_side<R: ReadAt>(
    spec: &KeySpec,
    side: &str,
    segments: &[SegmentReader<R>],
    columns: &[String],
    counts: bool,
) -> Result<()> {
    for s in segments {
        let ok = matches!(
            s.key_spec(),
            KeySpec::Exact { columns: c, bounds: None, .. } if c.as_slice() == columns
        );
        if !ok {
            return Err(Error::KeySpec(format!(
                "{} is built from the {side} side's exact key on {}, not from {}",
                spec.name(),
                columns.join(","),
                s.key_spec().identity()
            )));
        }
        if counts && !s.has_counts() {
            return Err(Error::KeySpec(format!(
                "{} stores counts, so the {side} side's exact key must count: declare {} there",
                spec.name(),
                side_spec(columns, true).identity()
            )));
        }
    }
    Ok(())
}

/// The manifests a pair index is built from (ADR 020).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pins {
    /// The left manifest sequence whose exact segments it reads.
    pub left: u64,
    /// The right manifest sequence it is pinned to.
    pub right: u64,
    /// The right manifest's content hash (BLAKE3 of its bytes as stored):
    /// what tells the manifest it was built at from another at the same
    /// sequence (an index deleted and rebuilt).
    pub right_manifest: Hash,
}

/// Caps on one pair segment (ADR 018): a segment is finished, and handed
/// to the caller, before the shared key that would take it past
/// `max_keys` keys or `max_bytes` bytes held, so a pair holds one open
/// segment at a time whatever `|L ∩ R|` is. A key's own record larger than
/// a cap gets a segment of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PairCaps {
    /// Shared keys one segment holds at most.
    pub max_keys: u64,
    /// Bytes one open segment holds (written blocks, index, open block)
    /// before it is finished.
    pub max_bytes: u64,
}

/// The identity of a live row-group set, as a join segment pins the
/// left's (`left_live`, `docs/format.md`, "Join keys"): BLAKE3 over each
/// id as 4 little-endian bytes, ascending. Two sets have one identity only
/// when they are equal, so a left file removed (its ids retired, none
/// added) changes it as surely as a file added.
#[must_use]
pub fn live_identity(live: &RoaringBitmap) -> Hash {
    let mut h = blake3::Hasher::new();
    for id in live {
        h.update(&id.to_le_bytes());
    }
    Hash(*h.finalize().as_bytes())
}

/// Finishes one pair segment: its one-sided key counts and the left row
/// groups it covers.
fn close(
    mut out: KeyedEmitter,
    (left_only, right_only): (u64, u64),
    left_range: Option<(u32, u32)>,
) -> Result<EncodedSegment> {
    out.set_join_totals(left_only, right_only)?;
    if let Some((lo, hi)) = left_range {
        out.processed(lo, hi)?;
    }
    out.finish()
}

/// Builds a join key's pair segments from both sides' exact segments (the
/// module docs), handing each to `emit` as soon as it is finished, in key
/// order: the segments split the shared keys by key range at `caps`, each
/// covering every left row group (`left_range`) and counting the
/// one-sided keys that fell in its stretch of the merge, so the segments
/// together hold each shared key once and their one-sided counts sum to
/// the pair's. `left_range` is the left row groups the pair covers,
/// inclusive (every live left row group, so the spec is complete at the
/// left's sequence), `None` for a left side with no row group; `pins` the
/// left manifest sequence the left segments are of and the right manifest
/// the pair is pinned to. Returns the number of segments (at least one).
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn build_pair<RL: ReadAt, RR: ReadAt>(
    spec: &KeySpec,
    left: &Side<'_, RL>,
    right: &Side<'_, RR>,
    left_range: Option<(u32, u32)>,
    pins: Pins,
    opts: &WriterOptions,
    caps: PairCaps,
    mut emit: impl FnMut(EncodedSegment) -> Result<()>,
) -> Result<u64> {
    check_spec(spec)?;
    let KeySpec::Join {
        columns,
        right_columns,
        counts,
        ..
    } = spec
    else {
        return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
    };
    check_side(spec, "left", left.segments, columns, *counts)?;
    check_side(spec, "right", right.segments, right_columns, *counts)?;
    let mut header = SegmentHeader::new(spec.clone());
    match (&left.types, &right.types) {
        (Some(l), Some(r)) => {
            check_comparable(spec, l, r)?;
            header.key_types = Some(l.clone());
            header.right_key_types = Some(r.clone());
        }
        (None, None) => {}
        _ => {
            return Err(Error::KeySpec(format!(
                "{}: the column types of both sides are needed, or of neither",
                spec.name()
            )))
        }
    }
    header.left_sequence = Some(pins.left);
    header.left_live = Some(live_identity(left.live));
    header.right_sequence = Some(pins.right);
    header.right_manifest = Some(pins.right_manifest);
    let mut out = KeyedEmitter::new(&header, opts)?;
    let mut segments = 0u64;
    let mut ls = LiveKeys {
        keys: MergedKeys::new(left.segments)?,
        live: left.live,
    };
    let mut rs = LiveKeys {
        keys: MergedKeys::new(right.segments)?,
        live: right.live,
    };
    let (mut left_only, mut right_only) = (0u64, 0u64);
    let (mut l, mut r) = (ls.next()?, rs.next()?);
    // A tuple holding a null element equals nothing (ADR 014): one-sided.
    let null = |k: &[u8]| tuple_has_null(k) == Some(true);
    loop {
        match (&l, &r) {
            (None, None) => break,
            (Some(_), None) => {
                left_only += 1;
                l = ls.next()?;
            }
            (None, Some(_)) => {
                right_only += 1;
                r = rs.next()?;
            }
            (Some((lk, lv)), Some((rk, rv))) => match lk.cmp(rk) {
                std::cmp::Ordering::Less => {
                    left_only += 1;
                    l = ls.next()?;
                }
                std::cmp::Ordering::Greater => {
                    right_only += 1;
                    r = rs.next()?;
                }
                std::cmp::Ordering::Equal => {
                    if null(lk) {
                        left_only += 1;
                        right_only += 1;
                    } else {
                        // Full: finish this segment before the next key, so
                        // the next one starts with it.
                        if out.entries() > 0
                            && (out.entries() >= caps.max_keys.max(1)
                                || out.held_bytes() >= caps.max_bytes)
                        {
                            let full =
                                std::mem::replace(&mut out, KeyedEmitter::new(&header, opts)?);
                            emit(close(full, (left_only, right_only), left_range)?)?;
                            segments += 1;
                            (left_only, right_only) = (0, 0);
                        }
                        if *counts {
                            out.push_join(lk, lv, rv)?;
                        } else {
                            // Counted exact keys serve an uncounted join key
                            // too; their counts are not stored.
                            let bare = |v: &KeyValue| KeyValue {
                                bitmap: v.bitmap.clone(),
                                chain: v.chain.clone(),
                                counts: None,
                                bounds: None,
                                triples: Vec::new(),
                                agg: None,
                            };
                            out.push_join(lk, &bare(lv), &bare(rv))?;
                        }
                    }
                    l = ls.next()?;
                    r = rs.next()?;
                }
            },
        }
    }
    emit(close(out, (left_only, right_only), left_range)?)?;
    Ok(segments + 1)
}

/// Reads one side's value record (`bitmap_len u32 | bitmap | [counts]`)
/// from the front of `c`, as the exact layout's value record is read.
fn read_side(c: &mut Cursor<'_>, counts: bool) -> Result<KeyValue> {
    let corrupt = |what: &str| Error::Other(format!("corrupt segment: {what}"));
    let b = c.bytes()?;
    if b.get(..4) != Some(&[0x3a, 0x30, 0x00, 0x00][..]) {
        return Err(corrupt("bitmap is not in the no-run serialized form"));
    }
    let bitmap = RoaringBitmap::deserialize_from(b)
        .map_err(|e| Error::Other(format!("corrupt segment bitmap: {e}")))?;
    if bitmap.is_empty() || bitmap.serialized_size() != b.len() {
        return Err(corrupt("keyed bitmap is empty or does not fill its record"));
    }
    let counts = if counts {
        let mut v = Vec::with_capacity(usize::try_from(bitmap.len()).unwrap_or(0).min(b.len()));
        for _ in 0..bitmap.len() {
            let n = c.leb128()?;
            if n == 0 {
                return Err(corrupt(
                    "a count of zero rows for a row group holding the key",
                ));
            }
            v.push(n);
        }
        Some(v)
    } else {
        None
    };
    Ok(KeyValue {
        bitmap,
        chain: crate::temporal::VersionChain::default(),
        counts,
        bounds: None,
        triples: Vec::new(),
        agg: None,
    })
}

/// Decodes a join value record: the left side's record, then the right's,
/// with nothing after. Anything else is refused as corrupt.
pub fn decode_pair(bytes: &[u8], counts: bool) -> Result<JoinValue> {
    let mut c = Cursor::new(bytes);
    let left = read_side(&mut c, counts)?;
    let right = read_side(&mut c, counts)?;
    if !c.is_done() {
        return Err(Error::Other(
            "corrupt segment: trailing bytes after a join value record".into(),
        ));
    }
    Ok(JoinValue { left, right })
}

/// Refuses a reader that is not a join key's segment.
fn check_join<R: ReadAt>(r: &SegmentReader<R>) -> Result<()> {
    if r.header().layout == Layout::KeyedJoin {
        Ok(())
    } else {
        Err(Error::KeySpec(format!(
            "{} is not a join key's segment",
            r.key_spec().identity()
        )))
    }
}

/// What one join segment's header pins: the manifests its pair was built
/// from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderPins {
    /// The left manifest sequence whose exact segments it read.
    pub left_sequence: u64,
    /// The left's live row-group set it covers ([`live_identity`]).
    pub left_live: Hash,
    /// The right manifest sequence it read.
    pub right_sequence: u64,
    /// The right manifest's content hash.
    pub right_manifest: Hash,
}

/// The pins of one join segment's header (a validated join header holds
/// all four).
pub fn header_pins<R: ReadAt>(r: &SegmentReader<R>) -> Result<HeaderPins> {
    check_join(r)?;
    let h = r.header();
    match (
        h.left_sequence,
        h.left_live,
        h.right_sequence,
        h.right_manifest,
    ) {
        (Some(left_sequence), Some(left_live), Some(right_sequence), Some(right_manifest)) => {
            Ok(HeaderPins {
                left_sequence,
                left_live,
                right_sequence,
                right_manifest,
            })
        }
        _ => Err(Error::Other(
            "corrupt segment: a join segment without its left and right pins".into(),
        )),
    }
}

/// The pins every segment of one join key shares; segments pinned to
/// different manifests are refused as one index. `None` for no segment.
pub fn pinned<R: ReadAt>(readers: &[SegmentReader<R>]) -> Result<Option<HeaderPins>> {
    let mut out: Option<HeaderPins> = None;
    for r in readers {
        let p = header_pins(r)?;
        if out.is_some_and(|o| o != p) {
            return Err(Error::Other(
                "corrupt index: the segments of one join key are pinned to different manifests"
                    .into(),
            ));
        }
        out = Some(p);
    }
    Ok(out)
}

/// `semijoin(keys)`: for each probe key both sides hold, its row groups on
/// each side. A key the pair index does not hold (on one side only, or on
/// neither) is absent; a probe with a null element matches nothing (ADR
/// 014). Holds the block index, one block per segment, and the values of
/// the keys asked (ADR 018).
pub fn semijoin<R: ReadAt, K: AsRef<[u8]>>(
    readers: &[SegmentReader<R>],
    keys: &[K],
) -> Result<BTreeMap<Vec<u8>, JoinValue>> {
    let mut out: BTreeMap<Vec<u8>, JoinValue> = BTreeMap::new();
    for r in readers {
        check_join(r)?;
        let counts = r.has_counts();
        r.visit_found(keys, |key, view, i| {
            let value = decode_pair(&r.value_bytes(view, i)?, counts)?;
            if out.insert(key.to_vec(), value).is_some() {
                return Err(Error::Other(
                    "corrupt index: two segments of one join key hold one key".into(),
                ));
            }
            Ok(())
        })?;
    }
    Ok(out)
}

/// Visits every shared key of a join key's segments with its value, in
/// key order within each segment, one block at a time (ADR 018).
pub fn for_each_pair<R: ReadAt>(
    readers: &[SegmentReader<R>],
    mut f: impl FnMut(&[u8], JoinValue) -> Result<()>,
) -> Result<()> {
    for r in readers {
        check_join(r)?;
        let Index::Keyed(blocks) = &r.index else {
            return Err(Error::KeySpec("a join segment has a keyed index".into()));
        };
        let counts = r.has_counts();
        for b in blocks {
            let body = r.block_body(b)?;
            let view = crate::segment::BlockView::parse(&body, b.n, r.checked())?;
            for i in 0..view.len() {
                f(
                    view.key(i)?,
                    decode_pair(&r.value_bytes(&view, i)?, counts)?,
                )?;
            }
        }
    }
    Ok(())
}

/// `overlap_stats()`: `|L ∩ R|`, `|L \ R|`, `|R \ L|`, and the row groups
/// per shared key on each side (with counts, the rows), exact, read from
/// the pair index alone. Reads every block once.
pub fn overlap_stats<R: ReadAt>(readers: &[SegmentReader<R>]) -> Result<OverlapStats> {
    let mut s = OverlapStats::default();
    let counted = readers.first().is_some_and(SegmentReader::has_counts);
    let mut rows = PairRows::default();
    let overflow = || Error::Other("a join key's row counts exceed 2^64".into());
    for r in readers {
        check_join(r)?;
        let (l, rr) = r
            .join_totals
            .ok_or_else(|| Error::Other("corrupt segment: join totals missing".into()))?;
        s.left_only = s.left_only.checked_add(l).ok_or_else(overflow)?;
        s.right_only = s.right_only.checked_add(rr).ok_or_else(overflow)?;
    }
    for_each_pair(readers, |_, v| {
        s.both += 1;
        let (lr, rr) = (v.left.bitmap.len(), v.right.bitmap.len());
        s.max_right_row_groups = s.max_right_row_groups.max(rr);
        s.sum_right_row_groups += rr;
        s.max_left_row_groups = s.max_left_row_groups.max(lr);
        s.sum_left_row_groups += lr;
        if let (Some(a), Some(b)) = (&v.left.counts, &v.right.counts) {
            let (a, b): (u64, u64) = (a.iter().sum(), b.iter().sum());
            rows.left = rows.left.checked_add(a).ok_or_else(overflow)?;
            rows.right = rows.right.checked_add(b).ok_or_else(overflow)?;
            let j = a.checked_mul(b).ok_or_else(overflow)?;
            rows.join = rows.join.checked_add(j).ok_or_else(overflow)?;
        }
        Ok(())
    })?;
    s.rows = counted.then_some(rows);
    Ok(s)
}

/// Every shared key of one join segment with its value, in key order: a
/// whole-segment read for verification and oracles, like
/// [`SegmentReader::keyed_entries`]; a lookup holds only the keys asked.
pub fn pair_entries<R: ReadAt>(r: &SegmentReader<R>) -> Result<Vec<(Vec<u8>, JoinValue)>> {
    let mut out = Vec::new();
    for_each_pair(std::slice::from_ref(r), |k, v| {
        out.push((k.to_vec(), v));
        Ok(())
    })?;
    Ok(out)
}

/// The one-sided key counts a join segment records: `(left_only,
/// right_only)`.
pub fn one_sided<R: ReadAt>(r: &SegmentReader<R>) -> Result<(u64, u64)> {
    check_join(r)?;
    r.join_totals
        .ok_or_else(|| Error::Other("corrupt segment: join totals missing".into()))
}
