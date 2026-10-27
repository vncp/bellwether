//! Comparison state and its memory accounting (ADR 018).
//!
//! Every structure that grows with the data is charged to a [`Meter`] before
//! it grows: hash tables by capacity (including the moment during a resize
//! when the old and the new table are both alive), keys and per-key totals
//! by their heap allocations rounded up to 16 bytes. A charge that would take
//! the meter past the budget is refused and nothing is allocated; the caller
//! then abandons the pass. The meter's peak is therefore never above the
//! budget, and the accounting over-approximates what the allocator holds.

use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;

use xxhash_rust::xxh3::Xxh3DefaultBuilder;

use crate::totals::Wide;

/// A charge was refused: the state would exceed the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Overflow;

/// Accounted bytes of comparison state, with a hard budget.
#[derive(Debug, Clone)]
pub(crate) struct Meter {
    budget: u64,
    used: u64,
    peak: u64,
}

impl Meter {
    pub(crate) fn new(budget: u64) -> Self {
        Self {
            budget,
            used: 0,
            peak: 0,
        }
    }

    /// Charges `bytes`, or refuses without charging.
    pub(crate) fn charge(&mut self, bytes: u64) -> Result<(), Overflow> {
        let next = self.used.checked_add(bytes).ok_or(Overflow)?;
        if next > self.budget {
            return Err(Overflow);
        }
        self.used = next;
        self.peak = self.peak.max(next);
        Ok(())
    }

    /// Returns `bytes` previously charged.
    pub(crate) fn release(&mut self, bytes: u64) {
        debug_assert!(bytes <= self.used, "released more than charged");
        self.used = self.used.saturating_sub(bytes);
    }

    pub(crate) fn used(&self) -> u64 {
        self.used
    }

    pub(crate) fn peak(&self) -> u64 {
        self.peak
    }
}

/// Heap bytes of an allocation of `n` bytes, rounded up to the allocator's
/// 16-byte granularity; zero for an empty allocation.
pub(crate) fn heap(n: usize) -> u64 {
    if n == 0 {
        0
    } else {
        (n as u64).div_ceil(16) * 16
    }
}

/// Bytes of a hash table with `capacity` slots of `slot` bytes: buckets
/// (capacity × 8/7 at the maximum load factor, plus one group of control
/// bytes) times the slot plus its control byte.
pub(crate) fn table_bytes(capacity: usize, slot: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    let buckets = (capacity as u64) * 8 / 7 + 16;
    buckets * (slot as u64 + 1)
}

/// A capacity that is at least what a full table of `capacity` grows to on
/// its next insert (it doubles its buckets). Used to charge a resize before
/// it happens, while both tables are alive.
pub(crate) fn grown_capacity(capacity: usize) -> usize {
    (2 * capacity + 1).max(8)
}

/// The most a [`MeteredMap`] with `slot`-byte slots charges for its table
/// while it grows to hold `len` entries: the largest of its resizes (old and
/// new table both charged) and its final table. The pass estimate uses it,
/// so the estimate and the meter follow one model.
pub(crate) fn table_peak(len: u64, slot: usize) -> u64 {
    let len = usize::try_from(len).unwrap_or(usize::MAX);
    let (mut cap, mut peak) = (0usize, 0u64);
    while cap < len {
        let next = grown_capacity(cap);
        peak = peak.max(table_bytes(cap, slot) + table_bytes(next, slot));
        cap = next;
    }
    peak.max(table_bytes(cap, slot))
}

/// A hash map from canonical bytes to `V` whose table and keys are charged
/// to a meter. The hasher has a fixed seed, so behaviour does not vary
/// between runs.
pub(crate) struct MeteredMap<V> {
    /// The map. Mutate values through it and record any change in the heap
    /// they own with [`charge_heap`] / [`release_heap`]
    /// (the fields are separate so a value can stay borrowed meanwhile).
    pub(crate) map: HashMap<Box<[u8]>, V, Xxh3DefaultBuilder>,
    charged_table: u64,
    /// Heap owned by keys and values, as charged.
    pub(crate) charged_heap: u64,
}

impl<V> MeteredMap<V> {
    pub(crate) fn new() -> Self {
        Self {
            map: HashMap::with_hasher(Xxh3DefaultBuilder),
            charged_table: 0,
            charged_heap: 0,
        }
    }

    const SLOT: usize = size_of::<(Box<[u8]>, V)>();

    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn get_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        self.map.get_mut(key)
    }

    /// Inserts a new key, charging its bytes and any table growth first.
    /// `extra` is heap owned by `value` (charged with the key). The key must
    /// not be present.
    pub(crate) fn insert_new(
        &mut self,
        meter: &mut Meter,
        key: &[u8],
        value: V,
        extra: u64,
    ) -> Result<&mut V, Overflow> {
        debug_assert!(!self.map.contains_key(key));
        let key_heap = heap(key.len());
        let old_cap = self.map.capacity();
        let growing = self.map.len() == old_cap;
        let new_table = if growing {
            table_bytes(grown_capacity(old_cap), Self::SLOT)
        } else {
            0
        };
        // During a resize the old table (already charged) and the new one
        // are both alive.
        meter.charge(key_heap + extra + new_table)?;
        self.map.insert(key.into(), value);
        self.charged_heap += key_heap + extra;
        if growing {
            let actual = table_bytes(self.map.capacity(), Self::SLOT);
            debug_assert!(actual <= new_table, "table grew past the charged bound");
            // Settle: the old table is freed and the new one costs `actual`.
            meter.release(self.charged_table + new_table.saturating_sub(actual));
            self.charged_table = actual.min(new_table);
        }
        Ok(self.map.get_mut(key).unwrap_or_else(|| unreachable!()))
    }

    /// Iterates the entries in unspecified order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&[u8], &V)> {
        self.map.iter().map(|(k, v)| (&**k, v))
    }

    /// Clears the map's entries, keeps its bucket table allocated for the
    /// next pass so a multi-pass run does not reallocate a second multi-GiB
    /// table in another virtual region, and returns only the entries' heap
    /// charge to the meter.
    pub(crate) fn clear(&mut self, meter: &mut Meter) {
        self.map.clear();
        meter.release(std::mem::take(&mut self.charged_heap));
    }

    /// Frees the map and returns its charge to the meter.
    pub(crate) fn free(self, meter: &mut Meter) {
        meter.release(self.charged_table + self.charged_heap);
    }
}

/// Charges heap a value is about to own (before allocating it).
pub(crate) fn charge_heap(
    charged_heap: &mut u64,
    meter: &mut Meter,
    bytes: u64,
) -> Result<(), Overflow> {
    meter.charge(bytes)?;
    *charged_heap += bytes;
    Ok(())
}

/// Returns heap a value no longer owns.
pub(crate) fn release_heap(charged_heap: &mut u64, meter: &mut Meter, bytes: u64) {
    debug_assert!(bytes <= *charged_heap);
    meter.release(bytes);
    *charged_heap -= bytes;
}

/// Per-group totals contributed by one key's compared rows on one side.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Contrib {
    /// `(group id, rows, one sum per total column)`.
    pub entries: Vec<(u32, u64, Box<[Wide]>)>,
}

impl Contrib {
    /// Heap bytes owned, as accounted.
    pub(crate) fn heap_bytes(&self) -> u64 {
        let vec = heap(self.entries.capacity() * size_of::<(u32, u64, Box<[Wide]>)>());
        let sums: u64 = self
            .entries
            .iter()
            .map(|(_, _, s)| heap(s.len() * size_of::<Wide>()))
            .sum();
        heap(size_of::<Self>()) + vec + sums
    }
}

/// One key's state on one side within a pass.
#[derive(Debug, Clone, Default)]
pub(crate) struct SideState {
    /// Rows of the key on this side, any version.
    pub rows: u64,
    /// The newest non-null version value, when `latest_by` is set.
    pub newest: Option<i64>,
    /// Rows in the compared version.
    pub compared: u64,
    /// Wrapping sum of the compared rows' row hashes.
    pub sum: u128,
    /// Totals of the compared rows by group, when `latest_by` and totals
    /// are both set (otherwise rows add to the group totals directly).
    pub contrib: Option<Box<Contrib>>,
}

/// One key's state on both sides.
#[derive(Debug, Clone, Default)]
pub(crate) struct KeyState {
    pub sides: [SideState; 2],
}

/// Totals for one group on both sides.
#[derive(Debug, Clone)]
pub(crate) struct GroupAcc {
    /// Compared rows per side.
    pub rows: [u64; 2],
    /// Per side, one sum per total column.
    pub sums: [Box<[Wide]>; 2],
}

/// Every group seen in the run, with its totals. Persists across passes.
pub(crate) struct Groups {
    index: MeteredMap<u32>,
    accs: Vec<GroupAcc>,
    charged_accs: u64,
    columns: usize,
    /// The group table may use at most this many bytes, so the per-key
    /// state always has room to shrink into by adding passes.
    limit: u64,
}

/// Why a group could not be added.
pub(crate) enum GroupError {
    /// The pass ran out of budget; more passes may help.
    Overflow,
    /// The group table alone is over its limit; more passes cannot help.
    TooMany(usize),
}

impl Groups {
    pub(crate) fn new(columns: usize, limit: u64) -> Self {
        Self {
            index: MeteredMap::new(),
            accs: Vec::new(),
            charged_accs: 0,
            columns,
            limit,
        }
    }

    fn charged(&self) -> u64 {
        self.index.charged_table + self.index.charged_heap + self.charged_accs
    }

    /// The id of `group`, adding it if new.
    pub(crate) fn intern(&mut self, meter: &mut Meter, group: &[u8]) -> Result<u32, GroupError> {
        if let Some(id) = self.index.get_mut(group) {
            return Ok(*id);
        }
        let id =
            u32::try_from(self.accs.len()).map_err(|_| GroupError::TooMany(self.accs.len()))?;
        let sums = heap(self.columns * size_of::<Wide>()) * 2;
        let vec_growth = if self.accs.len() == self.accs.capacity() {
            // A Vec doubles; old and new buffers are alive while it moves.
            heap(((2 * self.accs.capacity()).max(4)) * size_of::<GroupAcc>())
        } else {
            0
        };
        let before = meter.used();
        if self.charged() + sums + vec_growth + heap(group.len()) + 64 > self.limit {
            return Err(GroupError::TooMany(self.accs.len()));
        }
        meter
            .charge(sums + vec_growth)
            .map_err(|_| GroupError::Overflow)?;
        if let Err(Overflow) = self.index.insert_new(meter, group, id, 0) {
            meter.release(sums + vec_growth);
            return Err(GroupError::Overflow);
        }
        let old_cap = self.accs.capacity();
        self.accs.push(GroupAcc {
            rows: [0, 0],
            sums: [
                vec![Wide::default(); self.columns].into_boxed_slice(),
                vec![Wide::default(); self.columns].into_boxed_slice(),
            ],
        });
        self.charged_accs += sums;
        if vec_growth > 0 {
            let old = heap(old_cap * size_of::<GroupAcc>());
            let new = heap(self.accs.capacity() * size_of::<GroupAcc>());
            debug_assert!(
                new <= vec_growth,
                "group vector grew past the charged bound"
            );
            meter.release(old + vec_growth.saturating_sub(new));
            self.charged_accs = self.charged_accs - old + new.min(vec_growth);
        }
        debug_assert!(meter.used() >= before);
        Ok(id)
    }

    pub(crate) fn acc_mut(&mut self, id: u32) -> &mut GroupAcc {
        &mut self.accs[id as usize]
    }

    /// Groups in canonical byte order with their totals.
    pub(crate) fn sorted(&self) -> Vec<(&[u8], &GroupAcc)> {
        let mut out: Vec<(&[u8], &GroupAcc)> = self
            .index
            .iter()
            .map(|(k, id)| (k, &self.accs[*id as usize]))
            .collect();
        out.sort_by(|a, b| a.0.cmp(b.0));
        out
    }

    pub(crate) fn free(self, meter: &mut Meter) {
        meter.release(self.charged_accs);
        self.index.free(meter);
    }
}

/// The smallest `limit` keys seen with some value, charged to a meter.
pub(crate) struct Examples<T> {
    map: BTreeMap<Box<[u8]>, T>,
    limit: usize,
    charged: u64,
}

impl<T> Examples<T> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            limit,
            charged: 0,
        }
    }

    fn cost(key: &[u8]) -> u64 {
        // A B-tree slot holds the key box and the value; nodes are at least
        // half full, so a slot costs at most twice its size.
        heap(key.len()) + 2 * (size_of::<(Box<[u8]>, T)>() as u64)
    }

    /// Offers an example; keeps it if it is among the `limit` smallest keys.
    pub(crate) fn offer(
        &mut self,
        meter: &mut Meter,
        key: &[u8],
        value: T,
    ) -> Result<(), Overflow> {
        if self.limit == 0 {
            return Ok(());
        }
        if self.map.len() == self.limit {
            match self.map.last_key_value() {
                Some((last, _)) if **last > *key => {}
                _ => return Ok(()),
            }
        }
        debug_assert!(
            !self.map.contains_key(key),
            "a key is classified once per run"
        );
        let cost = Self::cost(key);
        meter.charge(cost)?;
        self.charged += cost;
        self.map.insert(key.into(), value);
        if self.map.len() > self.limit {
            if let Some((k, _)) = self.map.pop_last() {
                let c = Self::cost(&k);
                meter.release(c);
                self.charged -= c;
            }
        }
        Ok(())
    }

    pub(crate) fn into_sorted(self, meter: &mut Meter) -> Vec<(Box<[u8]>, T)> {
        meter.release(self.charged);
        self.map.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meter_refuses_past_the_budget_and_keeps_the_peak() {
        let mut m = Meter::new(100);
        m.charge(60).unwrap();
        assert_eq!(m.charge(41), Err(Overflow));
        assert_eq!(m.used(), 60);
        m.release(60);
        m.charge(100).unwrap();
        assert_eq!(m.peak(), 100);
    }

    #[test]
    fn metered_map_charges_growth_before_it_happens_and_frees_everything() {
        let mut m = Meter::new(u64::MAX);
        let mut map: MeteredMap<u64> = MeteredMap::new();
        for i in 0u64..10_000 {
            map.insert_new(&mut m, &i.to_be_bytes(), i, 0).unwrap();
            // The accounted table always covers the real one.
            assert!(map.charged_table >= table_bytes(map.map.capacity(), MeteredMap::<u64>::SLOT));
        }
        assert!(m.peak() >= m.used());
        map.free(&mut m);
        assert_eq!(m.used(), 0);
    }

    #[test]
    fn metered_map_clear_keeps_table_allocation_and_releases_entry_heap() {
        let mut m = Meter::new(u64::MAX);
        let mut map: MeteredMap<u64> = MeteredMap::new();
        for i in 0u64..1_000 {
            map.insert_new(&mut m, &i.to_be_bytes(), i, 32).unwrap();
        }
        let cap = map.map.capacity();
        let table = map.charged_table;
        assert!(table > 0);
        assert!(map.charged_heap > 0);
        map.clear(&mut m);
        assert_eq!(map.len(), 0);
        assert_eq!(map.map.capacity(), cap);
        assert_eq!(map.charged_heap, 0);
        assert_eq!(m.used(), table);
        for i in 1_000u64..2_000 {
            map.insert_new(&mut m, &i.to_be_bytes(), i, 32).unwrap();
        }
        assert_eq!(map.map.capacity(), cap);
        map.free(&mut m);
        assert_eq!(m.used(), 0);
    }

    #[test]
    fn a_refused_insert_allocates_nothing() {
        let mut m = Meter::new(2_000);
        let mut map: MeteredMap<u64> = MeteredMap::new();
        let mut inserted = 0u64;
        for i in 0u64..1_000 {
            if map.insert_new(&mut m, &i.to_be_bytes(), i, 0).is_err() {
                break;
            }
            inserted += 1;
        }
        assert_eq!(map.len() as u64, inserted);
        assert!(m.peak() <= 2_000);
    }

    #[test]
    fn examples_keep_the_smallest_keys() {
        let mut m = Meter::new(u64::MAX);
        let mut ex: Examples<u8> = Examples::new(3);
        for k in [9u8, 1, 7, 3, 5, 0] {
            ex.offer(&mut m, &[k], k).unwrap();
        }
        let kept: Vec<u8> = ex.into_sorted(&mut m).into_iter().map(|(_, v)| v).collect();
        assert_eq!(kept, [0, 1, 3]);
        assert_eq!(m.used(), 0);
    }
}
