//! Aggregate lookups over agg keys (B7): `sum`, `count`, `min`, `max` of
//! one value column per key over a key set, from the index.
//!
//! # Which keys answer
//!
//! An aggregate by columns `C` of value `v` is answered by one complete agg
//! key on `v` whose key columns `P` are all in `C` and which stores every
//! function asked (the one with the most columns; `P = C` when declared),
//! beside a complete spec on exactly `C` that stores counts
//! (`exact:C,counts`). The counts prove, per matching row group, whether the
//! agg key's cell for the key's projection onto `P` holds exactly the
//! lookup key's rows (`bellwether_core::agg::unanswered`); a row group where
//! it holds more rows (a finer lookup over a coarser agg key) is read
//! instead. When `P = C` every matching row group is answered and nothing
//! is read.
//!
//! # What the answer guarantees
//!
//! Per key: the exact rows (from the counts), and the aggregate over the
//! answered row groups, exact for counts, decimal and integer sums, minimum
//! and maximum (a float sum is a compensated `f64` sum, labelled with
//! [`bellwether_types::AGG_FLOAT_SUM_MODEL`]). The residual plan holds every
//! row group no key's cell answers, with an `In` residual over the keys
//! that have rows there: reading it and keeping the rows the residual keeps
//! adds exactly the rows the partial aggregates lack (a row group is read
//! for every key holding it, or for none). A key holding a null matches
//! nothing (ADR 014).

use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use bellwether_core::agg::{self as core_agg, AggCell, KeyCover};
use bellwether_core::segment::{ReadAt, SegmentReader};
use bellwether_store::StoreExt;
use bellwether_types::{AggregateFn, Error, KeySpec, Predicate, Result, ScanPlan};
use roaring::RoaringBitmap;

use super::{expanded, non_null_probes, overlapping_counts, Planner, TransientSpec};
use crate::keys::{project, zero_variants};

/// An aggregate lookup: `fn(value) group by columns where (columns) in
/// keys`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggLookup {
    /// Lookup columns.
    pub columns: Vec<String>,
    /// Canonical key tuples over `columns` at the dataset types.
    pub keys: Vec<Vec<u8>>,
    /// The aggregated column.
    pub value: String,
    /// The functions asked; `count` is always answered.
    pub functions: Vec<AggregateFn>,
}

/// One key's aggregate.
#[derive(Clone, Debug, PartialEq)]
pub struct KeyAggregate {
    /// The canonical key tuple.
    pub key: Vec<u8>,
    /// Its rows, its aggregate over the answered row groups, and the row
    /// groups still to read.
    pub cover: KeyCover,
}

/// The answer of [`Planner::aggregate`].
#[derive(Clone, Debug, PartialEq)]
pub struct Aggregated {
    /// The pinned manifest sequence.
    pub manifest_version: String,
    /// The agg key that answered.
    pub agg_spec: KeySpec,
    /// The spec whose counts proved coverage.
    pub counted_by: KeySpec,
    /// The value column's dataset type.
    pub value_type: DataType,
    /// Every non-null key asked, ascending and distinct.
    pub keys: Vec<KeyAggregate>,
    /// The row groups no cell answers, with an `In` residual over the keys
    /// holding rows there. Empty (no entry, no row group) when the index
    /// answered everything.
    pub residual: ScanPlan,
}

impl<S: StoreExt + ?Sized> Planner<S> {
    /// Answers `lookup` from the agg key and the counted key among `specs`
    /// that serve it (module docs). Reads the manifest and the value
    /// records of the keys asked; no data file is opened. An error names
    /// the declaration that is missing when none serves.
    pub fn aggregate(&self, specs: &[KeySpec], lookup: &AggLookup) -> Result<Aggregated> {
        self.aggregate_with_transient(specs, lookup, &[])
    }

    /// [`Planner::aggregate`], where a spec equal to one in `extra` uses
    /// that transient spec's in-memory segments (replay's pricing).
    #[allow(clippy::too_many_lines)]
    pub fn aggregate_with_transient(
        &self,
        specs: &[KeySpec],
        lookup: &AggLookup,
        extra: &[TransientSpec],
    ) -> Result<Aggregated> {
        let types = self.column_types(&lookup.columns)?;
        let value_type = self
            .schema
            .field_with_name(&lookup.value)
            .map_err(|_| {
                Error::KeySpec(format!(
                    "column {} is not in the dataset schema",
                    lookup.value
                ))
            })?
            .data_type()
            .clone();
        let agg = serving_agg(specs, lookup)
            .ok_or_else(|| no_agg(&self.manifest().source.root, specs, lookup))?;
        let counting = counting_spec(specs, &lookup.columns)
            .ok_or_else(|| no_counts(&self.manifest().source.root, agg, lookup))?;
        let segs = self.resolve(&[agg.clone(), counting.clone()], extra)?;
        let open = |i: usize| -> Result<Vec<_>> {
            segs[i]
                .sources
                .iter()
                .map(|src| self.open_segment(&segs[i].spec, src, false))
                .collect()
        };
        let answer = answer(
            (agg, &open(0)?),
            (counting, &open(1)?),
            self.locator.live(),
            &types,
            lookup,
        )?;
        let residual = self.finish(&answer.residual, answer.residual_predicate(lookup))?;
        Ok(Aggregated {
            manifest_version: self.sequence().to_string(),
            agg_spec: agg.clone(),
            counted_by: counting.clone(),
            value_type,
            keys: answer.keys,
            residual,
        })
    }

    /// Visits every key of the complete agg key `spec` in ascending order
    /// with its rows and its exact sum over the live row groups (keys with
    /// no live row are skipped): the per-key totals a reconcile reads from
    /// the index instead of the rows (B7). Holds one block per segment at a
    /// time (ADR 018). Refused for a spec that stores no sum or whose sums
    /// are floats, which are never exact.
    pub fn agg_totals(&self, spec: &KeySpec, f: &mut TotalsVisitor<'_>) -> Result<()> {
        if !core_agg::functions(spec).contains(&AggregateFn::Sum) {
            return Err(Error::KeySpec(format!(
                "{} stores no sum, so it has no totals to read",
                spec.identity()
            )));
        }
        let segs = self.resolve(std::slice::from_ref(spec), &[])?;
        let readers = segs[0]
            .sources
            .iter()
            .map(|src| self.open_segment(spec, src, false))
            .collect::<Result<Vec<_>>>()?;
        let live = self.locator.live();
        bellwether_core::merge::visit_merged_values(&readers, |key, value| {
            let cells = value.cells().ok_or_else(|| {
                Error::Other("corrupt segment: an agg value without aggregates".into())
            })?;
            let mut total = AggCell::empty();
            let mut any = false;
            for (rg, cell) in cells {
                if live.contains(rg.0) {
                    total.combine(cell)?;
                    any = true;
                }
            }
            if !any {
                return Ok(());
            }
            let Some(core_agg::AggSum::Int(sum)) = total.sum else {
                return Err(Error::KeySpec(format!(
                    "{} sums floats, which are never exact, so its totals cannot stand in for a \
                     reconcile's exact totals",
                    spec.identity()
                )));
            };
            f(key, total.rows, sum)
        })
    }
}

/// What [`Planner::agg_totals`] calls per key: the key, its rows, and its
/// exact sum over live row groups.
pub type TotalsVisitor<'f> = dyn FnMut(&[u8], u64, i128) -> Result<()> + 'f;

/// What [`answer`] computes from one agg key and the counted key beside it.
#[derive(Clone, Debug, PartialEq)]
pub struct AggAnswer {
    /// Every non-null key asked, ascending and distinct.
    pub keys: Vec<KeyAggregate>,
    /// The row groups no cell answers, for every key (their union).
    pub residual: RoaringBitmap,
    /// The keys with rows in `residual`, every float-zero variant, sorted
    /// and distinct: the `In` residual that keeps exactly their rows.
    pub read_keys: Vec<Vec<u8>>,
    /// Rows of the keys, from the counts (exact).
    pub rows: u64,
}

impl AggAnswer {
    /// The residual predicate a plan of [`Self::residual`] carries: `In`
    /// over [`Self::read_keys`] on the lookup's columns.
    #[must_use]
    pub fn residual_predicate(&self, lookup: &AggLookup) -> Predicate {
        Predicate::In {
            columns: lookup.columns.clone(),
            values: self.read_keys.clone(),
        }
    }
}

/// Answers `lookup` from the segments of `agg` (an agg key serving it,
/// [`serving_agg`]) and of `counting` (the spec counting rows on exactly
/// the lookup's columns, [`counting_spec`]) over the `live` row groups;
/// `types` are the lookup columns' dataset types. The one definition both
/// the index ([`Planner::aggregate`]) and replay's planner over transient
/// segments use, so a predicted aggregate plan is the planned one. Reads
/// the value records of the keys asked only.
#[allow(clippy::too_many_lines)] // the two steps of `cover`, then the plan
pub fn answer<R: ReadAt>(
    (agg, agg_readers): (&KeySpec, &[SegmentReader<R>]),
    (counting, count_readers): (&KeySpec, &[SegmentReader<R>]),
    live: &RoaringBitmap,
    types: &[(String, DataType)],
    lookup: &AggLookup,
) -> Result<AggAnswer> {
    let (mut keys, mut spans) = non_null_probes(&lookup.keys, types)?;
    // Ascending and distinct, so each key is answered once.
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_by(|a, b| keys[*a].cmp(&keys[*b]));
    order.dedup_by(|a, b| keys[*a] == keys[*b]);
    keys = order.iter().map(|&i| keys[i].clone()).collect();
    spans = order.iter().map(|&i| spans[i].clone()).collect();
    let positions = |of: &KeySpec| -> Vec<usize> {
        of.columns()
            .iter()
            .filter_map(|c| lookup.columns.iter().position(|x| x == c))
            .collect()
    };
    let (agg_pos, count_pos) = (positions(agg), positions(counting));
    let variants = |pos: &[usize]| -> Vec<Vec<Vec<u8>>> {
        keys.iter()
            .zip(&spans)
            .map(|(k, sp)| zero_variants(&project(k, sp, pos)))
            .collect()
    };
    let (agg_variants, count_variants) = (variants(&agg_pos), variants(&count_pos));
    let sorted = |v: &[Vec<Vec<u8>>]| -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = v.iter().flatten().cloned().collect();
        out.sort_unstable();
        out.dedup();
        out
    };
    // Counts per probe and live row group, each (probe, row group) from
    // one segment only.
    let mut counts: BTreeMap<Vec<u8>, BTreeMap<u32, u64>> = BTreeMap::new();
    let count_probes = sorted(&count_variants);
    for r in count_readers {
        let found = r.counts(&count_probes)?.ok_or_else(|| {
            Error::Other(format!(
                "corrupt index: {} is listed as storing counts and its segment stores none",
                counting.identity()
            ))
        })?;
        for (k, per_rg) in found {
            let m = counts.entry(k).or_default();
            for (rg, n) in per_rg {
                if live.contains(rg.0) && m.insert(rg.0, n).is_some() {
                    return Err(overlapping_counts(counting));
                }
            }
        }
    }
    let mut cells: BTreeMap<Vec<u8>, BTreeMap<u32, AggCell>> = BTreeMap::new();
    let agg_probes = sorted(&agg_variants);
    for r in agg_readers {
        for (k, per_rg) in r.agg_cells(&agg_probes)? {
            let m = cells.entry(k).or_default();
            for (rg, cell) in per_rg {
                if live.contains(rg.0) && m.insert(rg.0, cell).is_some() {
                    return Err(Error::Other(format!(
                        "corrupt index: two segments listed under {} aggregate the same key in \
                         row group {}, which no build commits; run `bellwether index` on the \
                         data folder to rebuild it, or restore the last good manifest from \
                         manifests/",
                        agg.identity(),
                        rg.0
                    )));
                }
            }
        }
    }
    // Every variant of a key is the same key under SQL equality: their
    // counts add and their cells combine, per row group.
    let mut per_key: Vec<(BTreeMap<u32, u64>, BTreeMap<u32, AggCell>)> =
        Vec::with_capacity(keys.len());
    for (cv, av) in count_variants.iter().zip(&agg_variants) {
        let mut c: BTreeMap<u32, u64> = BTreeMap::new();
        for v in cv {
            for (rg, n) in counts.get(v).into_iter().flatten() {
                *c.entry(*rg).or_insert(0) += n;
            }
        }
        let mut a: BTreeMap<u32, AggCell> = BTreeMap::new();
        for v in av {
            for (rg, cell) in cells.get(v).into_iter().flatten() {
                match a.get_mut(rg) {
                    Some(x) => x.combine(cell)?,
                    None => {
                        a.insert(*rg, cell.clone());
                    }
                }
            }
        }
        per_key.push((c, a));
    }
    let same = agg_pos.len() == lookup.columns.len();
    let mut residual = RoaringBitmap::new();
    for (c, a) in &per_key {
        residual |= core_agg::unanswered(c, a, same, agg)?;
    }
    let mut out = Vec::with_capacity(keys.len());
    let mut read_keys: Vec<&Vec<u8>> = Vec::new();
    let mut rows = 0u64;
    for (key, (c, a)) in keys.iter().zip(&per_key) {
        let cover = core_agg::cover(c, a, &residual)?;
        rows = rows
            .checked_add(cover.rows)
            .ok_or_else(|| Error::Other("row count overflows u64".into()))?;
        if !cover.residual.is_empty() {
            read_keys.push(key);
        }
        out.push(KeyAggregate {
            key: key.clone(),
            cover,
        });
    }
    Ok(AggAnswer {
        read_keys: expanded(read_keys.into_iter()),
        keys: out,
        residual,
        rows,
    })
}

/// The agg key among `specs` that serves `lookup`: on its value, storing
/// every function asked, its key columns all lookup columns; the one with
/// the most key columns, the first of those on a tie.
#[must_use]
pub fn serving_agg<'a>(specs: &'a [KeySpec], lookup: &AggLookup) -> Option<&'a KeySpec> {
    specs
        .iter()
        .filter(|s| match s {
            KeySpec::Agg {
                columns,
                value,
                functions,
            } => {
                *value == lookup.value
                    && columns.iter().all(|c| lookup.columns.contains(c))
                    && lookup.functions.iter().all(|f| functions.contains(f))
            }
            _ => false,
        })
        .fold(None, |best: Option<&KeySpec>, s| match best {
            Some(b) if b.columns().len() >= s.columns().len() => Some(b),
            _ => Some(s),
        })
}

/// The spec among `specs` that stores counts on exactly the lookup's
/// columns (in any order), so each probe is one stored key.
#[must_use]
pub fn counting_spec<'a>(specs: &'a [KeySpec], columns: &[String]) -> Option<&'a KeySpec> {
    specs.iter().find(|s| {
        matches!(s, KeySpec::Exact { .. } | KeySpec::Temporal { .. })
            && s.counts()
            && s.columns().len() == columns.len()
            && s.columns().iter().all(|c| columns.contains(c))
    })
}

fn functions_label(functions: &[AggregateFn]) -> String {
    AggregateFn::canonical(functions)
        .iter()
        .map(|f| f.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

/// The refusal when no agg key serves a lookup: what exists, and the
/// declaration that adds one.
fn no_agg(root: &str, specs: &[KeySpec], lookup: &AggLookup) -> Error {
    let have: Vec<String> = specs
        .iter()
        .filter(|s| matches!(s, KeySpec::Agg { .. }))
        .map(KeySpec::identity)
        .collect();
    let declare = format!(
        "bellwether index {root} --key 'exact:{cols},counts' --key 'agg:{cols}@{v}{{{fns}}}'",
        cols = lookup.columns.join(","),
        v = lookup.value,
        fns = functions_label(&lookup.functions)
    );
    Error::KeySpec(if have.is_empty() {
        format!(
            "no agg key aggregates {} here, so an aggregate by {} would need a scan; declare one \
             with `{declare}`",
            lookup.value,
            lookup.columns.join(",")
        )
    } else {
        format!(
            "none of the agg keys here ({}) aggregates {} by columns among {} with {}; declare \
             one with `{declare}`",
            have.join(", "),
            lookup.value,
            lookup.columns.join(","),
            functions_label(&lookup.functions)
        )
    })
}

/// The refusal when the agg key has no counted key on the lookup's columns
/// beside it.
fn no_counts(root: &str, agg: &KeySpec, lookup: &AggLookup) -> Error {
    let cols = lookup.columns.join(",");
    Error::KeySpec(format!(
        "{} answers an aggregate by {cols} only beside a key that counts rows on exactly {cols}, \
         which proves which of its aggregates cover each key; declare it with `bellwether index \
         {root} --key 'exact:{cols},counts'`",
        agg.identity()
    ))
}
