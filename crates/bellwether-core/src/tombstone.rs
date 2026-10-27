//! Tombstones on temporal and bitemporal keys (B8): supersession by a
//! retraction row.
//!
//! A temporal key `temporal:id@ts,tombstone=void` (or `tombstone=status=VOID`,
//! and the same on `temporal2`) names a column that marks a row as a
//! **tombstone**: a cancellation, a void, a restatement's retraction, a
//! delisting. A row is **live** when that column is not null and does not
//! hold the tombstone value (`false` for a boolean column); a null is not
//! live, as `where not col` keeps no null row (ADR 014).
//!
//! `latest(keys, live = true)` and `asof(keys, t, live = true)` answer the
//! operator `arg_max(ts, *) by key` followed by `where not retracted`: per
//! key, the rows at the key's newest time at or before `t` (ties kept), of
//! which only the live ones. A key whose newest row at or before `t` is a
//! tombstone has no row; so has a key whose only rows are tombstones; a key
//! retracted and then written again (a live row after the tombstone) has
//! the later live row. `live = false` is the lookup without tombstones.
//!
//! # What a tombstone segment stores
//!
//! A `keyed_tombstone` segment (the `tombstone` sidecar beside the key's
//! own segments, `docs/format.md` "Tombstones") holds, per key and per row
//! group holding a row of the key whose time is not null (both clocks, for
//! a bitemporal key), one [`Liveness`]: the newest time there, the newest
//! time of a live row there, and whether every row at the newest time is a
//! tombstone. A time is a [`Clock`]: `(ts, 0)` for a temporal key, `(valid,
//! known)` for a bitemporal one, compared in that order.
//!
//! # The pruning rule
//!
//! The plan of a lookup without tombstones is, per key, every row group
//! holding a row at the key's winning time `w`. With `live`, a row group
//! stays only when its newest live time is `>= w` ([`Liveness::keeps`]): a
//! row group whose live rows are all older than `w` holds no live row at
//! `w`, so it holds no answer. The rule never drops a row group holding a
//! live row at `w` (invariant 1), and for `latest` it is exact: a winning
//! row group's newest time is `w`, so it stays exactly when a live row is
//! at `w`. For `asof` at `t` below a row group's newest live time it keeps
//! the row group (it cannot tell), and the residual decides. The residual
//! ([`bellwether_types::Predicate::Live`], after `LatestPerKey`) drops every
//! tombstone row, so the rows returned are exact whatever was pruned.

use std::collections::BTreeMap;

use arrow::array::Array;
use arrow::datatypes::{DataType, Schema};
use bellwether_types::{Error, KeySpec, Result, Tombstone};
use roaring::RoaringBitmap;

use crate::encoding::{self, Value};
use crate::merge::MergedKeys;
use crate::segment::{corrupt, Cursor};
use crate::segment::{Layout, ReadAt, SegmentReader};

/// `segments[].kind` and `schema.sidecars[].kind` of a tombstone sidecar.
pub const TOMBSTONE_KIND: &str = "tombstone";

/// A row's time: `(ts, 0)` on a temporal key, `(valid, known)` on a
/// bitemporal key, each in its column's dataset unit; compared
/// lexicographically, as a bitemporal key ranks versions.
pub type Clock = (i64, i64);

/// One key's rows in one row group, as a tombstone segment stores them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Liveness {
    /// The newest time of the key's timed rows in the row group.
    pub newest: Clock,
    /// The newest time of a live row of the key there; `None` when every
    /// timed row of the key there is a tombstone (or has a null tombstone
    /// column).
    pub live: Option<Clock>,
    /// Whether every row of the key at `newest` there is a tombstone:
    /// exactly `live < newest` (or no live row).
    pub newest_is_tombstone: bool,
}

impl Liveness {
    /// Whether a lookup whose winning time for the key is `winner` keeps
    /// this row group: it may hold a live row at `winner` only when its
    /// newest live time reaches it.
    #[must_use]
    pub fn keeps(&self, winner: Clock) -> bool {
        self.live.is_some_and(|l| l >= winner)
    }

    /// The rules a stored entry meets (`docs/format.md`, "Tombstones"): a
    /// live time no later than the newest, and the flag exactly "no live
    /// row at the newest time".
    pub(crate) fn check(&self) -> Result<()> {
        let dead = match self.live {
            None => true,
            Some(l) if l <= self.newest => l < self.newest,
            Some(_) => return Err(corrupt("a liveness entry's live time is after its newest")),
        };
        if dead != self.newest_is_tombstone {
            return Err(corrupt(
                "a liveness entry's newest-is-tombstone flag disagrees with its times",
            ));
        }
        Ok(())
    }

    /// Appends the entry: `newest | live | flags u8`, each time one `i64`
    /// per clock (`clocks` 1 or 2), `live` all zero without a live row;
    /// `flags` bit 0 = has a live row, bit 1 = the newest row is a
    /// tombstone.
    pub(crate) fn write(&self, clocks: u8, out: &mut Vec<u8>) {
        let put = |out: &mut Vec<u8>, c: Clock| {
            out.extend_from_slice(&c.0.to_le_bytes());
            if clocks == 2 {
                out.extend_from_slice(&c.1.to_le_bytes());
            }
        };
        put(out, self.newest);
        put(out, self.live.unwrap_or((0, 0)));
        out.push(u8::from(self.live.is_some()) | (u8::from(self.newest_is_tombstone) << 1));
    }

    /// Reads a value record's `n` entries (one per row group of its
    /// bitmap) for a segment whose entries have `clocks` clocks, which must
    /// fill the rest of the record; `None` when `clocks` is 0 (no tombstone
    /// segment).
    pub(crate) fn read_n(c: &mut Cursor<'_>, n: usize, clocks: u8) -> Result<Option<Vec<Self>>> {
        if clocks == 0 {
            return Ok(None);
        }
        let per = 16 * usize::from(clocks) + 1;
        if n.saturating_mul(per) != c.remaining() {
            return Err(corrupt(
                "tombstone liveness entries do not fill the value record",
            ));
        }
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(Self::read(c, clocks)?);
        }
        Ok(Some(v))
    }

    /// Reads one entry written by [`Liveness::write`], refusing unknown
    /// flag bits, a live time without the flag (or the flag without one),
    /// and an entry that breaks [`Liveness::check`].
    pub(crate) fn read(c: &mut Cursor<'_>, clocks: u8) -> Result<Self> {
        let i64_at = |c: &mut Cursor<'_>| -> Result<i64> {
            Ok(i64::from_le_bytes(
                c.take(8)?.try_into().map_err(|_| corrupt("i64"))?,
            ))
        };
        let clock = |c: &mut Cursor<'_>| -> Result<Clock> {
            let a = i64_at(c)?;
            let b = if clocks == 2 { i64_at(c)? } else { 0 };
            Ok((a, b))
        };
        let newest = clock(c)?;
        let live = clock(c)?;
        let flags = c.u8()?;
        if flags > 0b11 {
            return Err(corrupt("a liveness entry has unknown flag bits"));
        }
        let has_live = flags & 1 == 1;
        if !has_live && live != (0, 0) {
            return Err(corrupt(
                "a liveness entry without a live row stores a live time",
            ));
        }
        let e = Self {
            newest,
            live: has_live.then_some(live),
            newest_is_tombstone: flags & 0b10 != 0,
        };
        e.check()?;
        Ok(e)
    }
}

/// The liveness of one key in one row group while a writer reads its rows.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LiveAcc {
    newest: Clock,
    live_at_newest: bool,
    live: Option<Clock>,
}

impl LiveAcc {
    pub(crate) fn new(time: Clock, live: bool) -> Self {
        Self {
            newest: time,
            live_at_newest: live,
            live: live.then_some(time),
        }
    }

    pub(crate) fn push(&mut self, time: Clock, live: bool) {
        match time.cmp(&self.newest) {
            std::cmp::Ordering::Greater => {
                self.newest = time;
                self.live_at_newest = live;
            }
            std::cmp::Ordering::Equal => self.live_at_newest |= live,
            std::cmp::Ordering::Less => {}
        }
        if live && self.live.is_none_or(|l| time > l) {
            self.live = Some(time);
        }
    }

    pub(crate) fn finish(self) -> Liveness {
        Liveness {
            newest: self.newest,
            live: self.live,
            newest_is_tombstone: !self.live_at_newest,
        }
    }
}

/// The clocks of `spec`'s liveness entries: 1 for a temporal key, 2 for a
/// bitemporal key, 0 for any other spec.
#[must_use]
pub fn clocks(spec: &KeySpec) -> u8 {
    match spec {
        KeySpec::Temporal { .. } => 1,
        KeySpec::Bitemporal { .. } => 2,
        _ => 0,
    }
}

/// The dataset type of `spec`'s tombstone column in `schema`, after the
/// rules a tombstone meets: a temporal or bitemporal spec; a column of the
/// dataset that is not a key or time column of the spec; with no value, a
/// boolean column; with a value, a boolean, integer, or string column (or a
/// dictionary of one) the value converts to exactly. Each refusal says what
/// to declare instead.
pub fn column_type(spec: &KeySpec, schema: &Schema) -> Result<DataType> {
    let Some(t) = spec.tombstone() else {
        return Err(Error::KeySpec(format!(
            "{} has no tombstone column",
            spec.identity()
        )));
    };
    let mut times: Vec<&String> = Vec::new();
    match spec {
        KeySpec::Temporal { ts_column, .. } => times.push(ts_column),
        KeySpec::Bitemporal {
            valid_column,
            known_column,
            ..
        } => {
            times.push(valid_column);
            times.push(known_column);
        }
        _ => {
            return Err(Error::KeySpec(format!(
                "a tombstone is a parameter of a temporal or temporal2 key, and {} is neither",
                spec.name()
            )))
        }
    }
    if spec.columns().contains(&t.column) || times.contains(&&t.column) {
        return Err(Error::KeySpec(format!(
            "{}: the tombstone column {:?} is a key or time column of the key; name the column \
             that marks a row as retracted (a boolean, or a status column with \
             tombstone=<column>=<value>)",
            spec.identity(),
            t.column
        )));
    }
    let dt = schema
        .field_with_name(&t.column)
        .map_err(|_| {
            Error::KeySpec(format!(
                "{}: the tombstone {}",
                spec.identity(),
                encoding::not_in_schema(schema, &t.column)
            ))
        })?
        .data_type()
        .clone();
    tombstone_bytes(t, &dt).map_err(|e| match e {
        Error::KeySpec(m) => Error::KeySpec(format!("{}: {m}", spec.identity())),
        other => other,
    })?;
    Ok(dt)
}

/// The canonical encoding (ADR 016) of the value that marks a tombstone in
/// a column of dataset type `dt`: `true` for the boolean form, else the
/// written value converted to `dt` exactly (refused when it does not
/// convert, or when `dt` is not a boolean, integer, or string type).
pub fn tombstone_bytes(t: &Tombstone, dt: &DataType) -> Result<Vec<u8>> {
    let value_type = match dt {
        DataType::Dictionary(_, v) => v.as_ref(),
        other => other,
    };
    let mut out = Vec::new();
    let Some(v) = t.value.as_deref() else {
        if *value_type != DataType::Boolean {
            return Err(Error::KeySpec(format!(
                "tombstone={} needs a boolean column, and {} is {dt}; name the value that marks a \
                 tombstone with tombstone={}=<value>",
                t.column, t.column, t.column
            )));
        }
        encoding::encode_value_as(&Value::Boolean(true), dt, &mut out)?;
        return Ok(out);
    };
    let refuse = |why: String| {
        Error::KeySpec(format!(
            "tombstone={}={v}: {why}; write a value of the column's type",
            t.column
        ))
    };
    let value = match value_type {
        DataType::Boolean => match v {
            "true" => Value::Boolean(true),
            "false" => Value::Boolean(false),
            _ => {
                return Err(refuse(format!(
                    "{} is boolean, so the value is true or false",
                    t.column
                )))
            }
        },
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Value::Int(v.parse().map_err(|_| {
                refuse(format!("{} is {dt}, and {v:?} is not an integer", t.column))
            })?)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Value::UInt(v.parse().map_err(|_| {
                refuse(format!(
                    "{} is {dt}, and {v:?} is not an unsigned integer",
                    t.column
                ))
            })?)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Value::Utf8(v),
        _ => {
            return Err(Error::KeySpec(format!(
                "the tombstone column {} is {dt}; a tombstone column is a boolean \
                 (tombstone={}), or an integer or string column with the value that marks a \
                 tombstone (tombstone={}=<value>)",
                t.column, t.column, t.column
            )))
        }
    };
    encoding::encode_value_as(&value, dt, &mut out)
        .map_err(|e| refuse(format!("it does not convert to {dt} ({e})")))?;
    Ok(out)
}

/// Tells live rows from tombstones in batches of one dataset.
#[derive(Clone, Debug)]
pub struct Matcher {
    column: String,
    declared: DataType,
    tombstone: Vec<u8>,
}

impl Matcher {
    /// The matcher of `spec`'s tombstone over a dataset whose schema is
    /// `schema` ([`column_type`]).
    pub fn for_spec(spec: &KeySpec, schema: &Schema) -> Result<Self> {
        let declared = column_type(spec, schema)?;
        let t = spec
            .tombstone()
            .ok_or_else(|| Error::KeySpec(format!("{} has no tombstone", spec.identity())))?;
        Ok(Self {
            column: t.column.clone(),
            tombstone: tombstone_bytes(t, &declared)?,
            declared,
        })
    }

    /// The tombstone column.
    #[must_use]
    pub fn column(&self) -> &str {
        &self.column
    }

    /// The column's dataset type.
    #[must_use]
    pub fn declared(&self) -> &DataType {
        &self.declared
    }

    /// The canonical encoding of the value that marks a tombstone.
    #[must_use]
    pub fn tombstone(&self) -> &[u8] {
        &self.tombstone
    }

    /// Whether row `row` of `array` (the tombstone column of a batch) is
    /// live: not null, and not the tombstone value once converted to the
    /// dataset type. `buf` is scratch space.
    pub fn is_live(&self, array: &dyn Array, row: usize, buf: &mut Vec<u8>) -> Result<bool> {
        buf.clear();
        Ok(
            encoding::encode_array_value_as(array, row, &self.declared, buf)?
                && buf.as_slice() != self.tombstone.as_slice(),
        )
    }
}

/// Whether a row whose tombstone column encodes to `value` (`None`: null)
/// is live under the residual `Live { tombstone }`: not null, and not the
/// tombstone.
#[must_use]
pub fn is_live_value(value: Option<&[u8]>, tombstone: &[u8]) -> bool {
    value.is_some_and(|v| v != tombstone)
}

/// The tombstone segments' entries of a key, by row group.
pub type KeyLiveness = BTreeMap<u32, Liveness>;

/// Every stored entry of each of `probes` present, across `readers`, the
/// tombstone segments of one spec (row groups united; a (key, row group)
/// held by two segments is refused as corrupt, since a build indexes each
/// row group once). Holds each segment's block index, one block, and the
/// entries of the keys asked (ADR 018).
pub fn liveness<R: ReadAt, K: AsRef<[u8]>>(
    readers: &[SegmentReader<R>],
    probes: &[K],
) -> Result<BTreeMap<Vec<u8>, KeyLiveness>> {
    let mut out: BTreeMap<Vec<u8>, KeyLiveness> = BTreeMap::new();
    for r in readers {
        if r.header().layout != Layout::KeyedTombstone {
            return Err(Error::KeySpec(format!(
                "{} is not a tombstone segment",
                r.key_spec().identity()
            )));
        }
        for (key, value) in r.get(probes)? {
            let entries = value.liveness.as_deref().ok_or_else(|| {
                corrupt("a tombstone segment holds a value without liveness entries")
            })?;
            let per_rg = out.entry(key).or_default();
            for (rg, e) in value.bitmap.iter().zip(entries) {
                if per_rg.insert(rg, *e).is_some() {
                    return Err(corrupt(&format!(
                        "two tombstone segments of one key both hold row group {rg}"
                    )));
                }
            }
        }
    }
    Ok(out)
}

/// The row groups of `row_groups` (a key's winning row groups at its
/// winning time `winner`) a lookup with `live` keeps ([`Liveness::keeps`]),
/// reading each row group's entries of `variants`: the stored keys one
/// queried key matches (both encodings of a float zero are one key under
/// SQL equality); a row group stays when any variant's entry keeps it.
/// Every winning row group holds a timed row of the key at `winner`, so
/// some variant's entry there has a newest time of at least `winner`; a
/// row group with none is refused as a corrupt index, never pruned or kept
/// on a guess.
pub fn prune(
    row_groups: &RoaringBitmap,
    winner: Clock,
    variants: &[&KeyLiveness],
) -> Result<RoaringBitmap> {
    let mut kept = RoaringBitmap::new();
    for rg in row_groups {
        let entries: Vec<&Liveness> = variants.iter().filter_map(|m| m.get(&rg)).collect();
        if !entries.iter().any(|e| e.newest >= winner) {
            return Err(corrupt(&format!(
                "row group {rg} holds a version of a key at a time its tombstone segments do not \
                 hold there"
            )));
        }
        if entries.iter().any(|e| e.keeps(winner)) {
            kept.insert(rg);
        }
    }
    Ok(kept)
}

/// How many keys of a tombstone spec's segments have a newest row that is
/// a tombstone, over the row groups in `live`: per key, its newest time
/// across those row groups, and whether every row at that time is a
/// tombstone. The number that says whether tombstones pay (`why`'s
/// `tombstone` column): those keys answer a `live` lookup with no row and
/// no row group.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NewestTombstones {
    /// Keys with a timed row in a live row group.
    pub keys: u64,
    /// Of those, keys whose newest row is a tombstone.
    pub newest_is_tombstone: u64,
}

/// [`NewestTombstones`] over `readers`, the tombstone segments of one spec:
/// a merge over their sorted keys holding one block per segment (ADR 018).
pub fn newest_tombstones<R: ReadAt>(
    readers: &[SegmentReader<R>],
    live: &RoaringBitmap,
) -> Result<NewestTombstones> {
    let mut out = NewestTombstones::default();
    if readers.is_empty() {
        return Ok(out);
    }
    let mut keys = MergedKeys::new(readers)?;
    while let Some((_, value)) = keys.next_key()? {
        let entries = value
            .liveness
            .as_deref()
            .ok_or_else(|| corrupt("a tombstone segment holds a value without liveness entries"))?;
        let mut newest: Option<(Clock, bool)> = None;
        for (rg, e) in value.bitmap.iter().zip(entries) {
            if !live.contains(rg) {
                continue;
            }
            newest = Some(match newest {
                Some((t, dead)) if t > e.newest => (t, dead),
                Some((t, dead)) if t == e.newest => (t, dead && e.newest_is_tombstone),
                _ => (e.newest, e.newest_is_tombstone),
            });
        }
        if let Some((_, dead)) = newest {
            out.keys += 1;
            out.newest_is_tombstone += u64::from(dead);
        }
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn the_accumulator_tracks_the_newest_and_the_newest_live_time() {
        let mut a = LiveAcc::new((5, 0), true);
        a.push((7, 0), false);
        a.push((6, 0), true);
        assert_eq!(
            a.finish(),
            Liveness {
                newest: (7, 0),
                live: Some((6, 0)),
                newest_is_tombstone: true
            }
        );
        // a tie at the newest time with one live row: the newest is live
        let mut b = LiveAcc::new((7, 0), false);
        b.push((7, 0), true);
        assert!(!b.finish().newest_is_tombstone);
        // only tombstones
        let c = LiveAcc::new((3, 1), false).finish();
        assert_eq!(c.live, None);
        assert!(c.newest_is_tombstone);
    }

    #[test]
    fn keeps_a_row_group_exactly_when_its_live_rows_reach_the_winner() {
        let e = Liveness {
            newest: (9, 0),
            live: Some((5, 0)),
            newest_is_tombstone: true,
        };
        assert!(e.keeps((5, 0)));
        assert!(e.keeps((4, 0)));
        assert!(!e.keeps((6, 0)));
        assert!(!e.keeps((9, 0)));
        let none = Liveness {
            newest: (9, 0),
            live: None,
            newest_is_tombstone: true,
        };
        assert!(!none.keeps((i64::MIN, i64::MIN)));
    }

    #[test]
    fn entries_round_trip_and_inconsistent_ones_are_refused() {
        for clocks in [1u8, 2] {
            for e in [
                Liveness {
                    newest: (9, if clocks == 2 { 4 } else { 0 }),
                    live: Some((5, 0)),
                    newest_is_tombstone: true,
                },
                Liveness {
                    newest: (9, 0),
                    live: Some((9, 0)),
                    newest_is_tombstone: false,
                },
                Liveness {
                    newest: (-1, 0),
                    live: None,
                    newest_is_tombstone: true,
                },
            ] {
                let mut b = Vec::new();
                e.write(clocks, &mut b);
                assert_eq!(b.len(), 16 * usize::from(clocks) + 1);
                let mut c = Cursor::new(&b);
                assert_eq!(Liveness::read(&mut c, clocks).unwrap(), e);
                assert!(c.is_done());
            }
        }
        let bad = |e: Liveness| {
            let mut b = Vec::new();
            e.write(1, &mut b);
            Liveness::read(&mut Cursor::new(&b), 1).is_err()
        };
        // live after newest; the flag set with a live row at the newest; the
        // flag clear without a live row
        assert!(bad(Liveness {
            newest: (1, 0),
            live: Some((2, 0)),
            newest_is_tombstone: false
        }));
        assert!(bad(Liveness {
            newest: (2, 0),
            live: Some((2, 0)),
            newest_is_tombstone: true
        }));
        assert!(bad(Liveness {
            newest: (2, 0),
            live: None,
            newest_is_tombstone: false
        }));
        // unknown flag bits; a live time stored without the flag
        let mut b = Vec::new();
        Liveness {
            newest: (2, 0),
            live: None,
            newest_is_tombstone: true,
        }
        .write(1, &mut b);
        let mut flags = b.clone();
        *flags.last_mut().unwrap() |= 0b100;
        assert!(Liveness::read(&mut Cursor::new(&flags), 1).is_err());
        let mut stray = b;
        stray[8] = 1;
        assert!(Liveness::read(&mut Cursor::new(&stray), 1).is_err());
    }

    #[test]
    fn tombstone_values_convert_exactly_or_are_refused() {
        let t = |column: &str, value: Option<&str>| Tombstone {
            column: column.into(),
            value: value.map(str::to_string),
        };
        assert!(tombstone_bytes(&t("void", None), &DataType::Boolean).is_ok());
        assert!(tombstone_bytes(&t("void", None), &DataType::Utf8).is_err());
        assert!(tombstone_bytes(&t("s", Some("VOID")), &DataType::Utf8).is_ok());
        assert!(tombstone_bytes(&t("s", Some("3")), &DataType::Int8).is_ok());
        assert!(tombstone_bytes(&t("s", Some("300")), &DataType::Int8).is_err());
        assert!(tombstone_bytes(&t("s", Some("-1")), &DataType::UInt32).is_err());
        assert!(tombstone_bytes(&t("s", Some("x")), &DataType::Int64).is_err());
        assert!(tombstone_bytes(&t("s", Some("yes")), &DataType::Boolean).is_err());
        assert!(tombstone_bytes(&t("s", Some("1.5")), &DataType::Float64).is_err());
        // an Int32 column's 3 and an Int64 column's 3 encode alike
        assert_eq!(
            tombstone_bytes(&t("s", Some("3")), &DataType::Int32).unwrap(),
            tombstone_bytes(&t("s", Some("3")), &DataType::Int64).unwrap()
        );
        assert!(is_live_value(Some(b"a"), b"b"));
        assert!(!is_live_value(Some(b"b"), b"b"));
        assert!(!is_live_value(None, b"b"));
    }
}
