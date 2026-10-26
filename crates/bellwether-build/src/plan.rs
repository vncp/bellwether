//! The scan plan builder (ADR 011): from key specs and a lookup to the files
//! and row groups to read, with exact bytes and a residual predicate.
//!
//! # Which specs serve a lookup
//!
//! A lookup names its columns `C`; a key spec over columns `P` serves it
//! when
//!
//! 1. every column of `P` is in `C`: each key tuple is projected onto `P`,
//!    in `P`'s order, and looked up exactly (exact, temporal, and bloom
//!    specs; a range spec is the case `P = [c]`); or else
//! 2. `C` is a strict prefix of `P` and the spec is exact or temporal: each
//!    key tuple is a prefix lookup (ADR 002).
//!
//! For each key tuple the row groups of every serving spec are intersected,
//! then the per-key sets are unioned. An empty spec list is a raw scan of
//! every live row group; a non-empty list of which none serves is an error.
//! No key plans no row group, with an empty `In`, never a full scan. A probe
//! containing a null matches nothing (ADR 014): it is dropped before any
//! lookup, point, prefix or projection, and from the residual; a range
//! with a null bound selects nothing.
//!
//! Probes are checked element by element against the dataset types (tag,
//! decimal width, time-unit tag), but raw bytes cannot carry a decimal's
//! scale or the unit a timestamp's value was computed in: encode probes
//! with [`Planner::encode_keys`] or [`Planner::encode_tuple`].
//! Every spec must be declared exactly as given and complete at the pinned
//! manifest sequence (ADR 012): an incomplete spec is an error, never a
//! partial plan.
//!
//! # What the plan guarantees
//!
//! Every live row group holding a queried key is in the plan (invariant 1).
//! Bitmap ids that a tombstone retired are dropped (their file version is
//! gone, ADR 013); an id the manifest never assigned is an error. Bytes are
//! exact sums of `RowGroupMeta.bytes` (Parquet footers) or of the recorded
//! chunk lengths (text). Planned files are checked against their
//! fingerprints, so a plan over a drifted file is an error.
//!
//! # Residuals
//!
//! - exact lookups: `In` over `C`, with both encodings of a float zero;
//! - latest and asof: `In` over `C` restricted to keys that have a version,
//!   then (asof only) `Between` on the timestamp column up to the asof
//!   instant, then `NotOlderThan` with each key's index-wide winning
//!   timestamp, then `LatestPerKey`, in that order. The bound comes from
//!   every segment of the temporal spec, so a row superseded by a version in
//!   a row group another spec pruned is still dropped (invariant 5);
//! - lookbacks: `In` over `C` restricted to keys with a live version, then
//!   `NotOlderThan` with each key's `newest − lookback` (`newest` is the
//!   key's newest live version, so nothing newer is live). No
//!   `LatestPerKey`: every version in the key's window is returned. The
//!   plan holds every live row group with a version of a key in its window;
//! - windows: `In` over `C`, then `Between` on the time column;
//! - range lookups: `Between`, widened for signed zero.
//!
//! # Order
//!
//! [`Planner::plan`] lists entries by path. [`Planner::plan_ordered`]
//! plans the same row groups, then orders entries by a range key on a
//! timestamp or date column: the one such complete spec, or, when there
//! are several, the one on the lookup's own time column (a window's column,
//! or the temporal spec's timestamp). Each entry sorts by the smallest
//! row-group minimum among its planned row groups, as the range key's
//! segments record it (a row group whose every value is null has none and
//! sorts last; ties by path). Row groups inside an entry stay in index
//! order, because an entry names its row groups as a set. The result is
//! [`PlanOrder::Ordered`] only when that reading order visits every planned
//! row group in non-decreasing minimum; otherwise it is
//! [`PlanOrder::Unspecified`] with the reason (two files overlap in time, or
//! a file's own row groups are out of time order), never a claimed order
//! that does not hold.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef};
use bellwether_core::encoding::{self, tag, KeyColumns, Value};
use bellwether_core::segment::ReadAt;
use bellwether_core::sidecar::{
    bitemporal_winner, BitemporalHit, RgBounds, SidecarDef, SidecarKind, SidecarReader, Triple,
};
use bellwether_core::temporal::{self, LookbackHit, VersionChain, VersionHit};
use bellwether_core::SegmentReader;
use bellwether_source::{
    fingerprint_with_policy, FingerprintPolicy, LocalFs, ParquetSource, Partitioning,
};
use bellwether_store::{Manifest, SegmentRef, Snapshot, StoreExt};
use bellwether_types::{
    ColumnBytes, Error, Hash, KeySpec, Predicate, Quantity, Result, RowGroupId, ScanEntry,
    ScanPlan, Source,
};
use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::config::{columns_to_schema, parse_policy, SourceKind};
use crate::coverage::{completeness, segment_of, sidecar_completeness, Locator};
use crate::keys::{encode_ts, project, split_checked, widen_zero_bounds, zero_variants};
use crate::seg_io::SegmentBytes;
use crate::sources::parquet_source;
use crate::target::declared_sidecars;

/// What to look up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// Rows whose `columns` tuple is one of `keys`. Each key is a canonical
    /// tuple over `columns` at the dataset types (see
    /// [`Planner::encode_keys`]).
    Keys {
        /// Lookup columns.
        columns: Vec<String>,
        /// Canonical key tuples.
        keys: Vec<Vec<u8>>,
    },
    /// The newest version of each key, per the one temporal spec that
    /// serves `columns`.
    Latest {
        /// Lookup columns; they must include every column of the temporal
        /// spec.
        columns: Vec<String>,
        /// Canonical key tuples.
        keys: Vec<Vec<u8>>,
    },
    /// The version of each key current at `ts`.
    AsOf {
        /// Lookup columns.
        columns: Vec<String>,
        /// Canonical key tuples.
        keys: Vec<Vec<u8>>,
        /// The instant, in the timestamp column's dataset unit (days for
        /// `Date32`, the integer itself for an integer column).
        ts: i64,
    },
    /// Every version of each key in its own lookback window `[newest −
    /// lookback, newest]`, `newest` being the key's newest live version
    /// (index-wide, live row groups only). The versions come from the one
    /// temporal spec whose columns are all in `columns` (its version
    /// chains), or, with none, from the one complete `bounds` sidecar on
    /// columns of `columns` (each key's own time range per row group; see
    /// [`Planner::lookback_column`]). Bounds inclusive, in the timestamp
    /// column's dataset unit; the lower bound is clamped to the smallest
    /// value the column holds. A window ending at a fixed instant is a
    /// [`Lookup::Window`].
    Lookback {
        /// Lookup columns.
        columns: Vec<String>,
        /// Canonical key tuples.
        keys: Vec<Vec<u8>>,
        /// The window's length, `>= 0`, in the timestamp column's unit.
        lookback: i64,
    },
    /// Rows whose `column` lies in `[lo, hi]`, both canonical encodings of
    /// one value of the column's dataset type.
    Between {
        /// The column.
        column: String,
        /// Inclusive lower bound.
        lo: Vec<u8>,
        /// Inclusive upper bound.
        hi: Vec<u8>,
    },
    /// Rows whose `columns` tuple is one of `keys` and whose `ts_column`
    /// lies in `[lo, hi]`, both in the column's dataset unit (as
    /// [`Lookup::AsOf`] takes its instant). Every source that can bound the
    /// window takes part, and their sets are intersected per key (each is a
    /// superset of the row groups holding a matching row, so the
    /// intersection is too): the key specs on `columns`; range specs on
    /// `ts_column` (or exact and temporal specs led by it); the version
    /// chains of a temporal spec on `columns@ts_column`, which keep only the
    /// row groups holding a version of the key inside the window; every
    /// complete `bounds` sidecar on `columns@ts_column`, which keeps a row
    /// group only when the key's own time range there overlaps the window;
    /// and, when the manifest's partitioning makes `ts_column` a column of
    /// every file (hive directories or a path template), the files whose
    /// path value lies in the window.
    Window {
        /// Lookup columns.
        columns: Vec<String>,
        /// Canonical key tuples.
        keys: Vec<Vec<u8>>,
        /// The time column.
        ts_column: String,
        /// Inclusive lower bound, in the column's unit.
        lo: i64,
        /// Inclusive upper bound, in the column's unit.
        hi: i64,
    },
    /// Per key, the newest row by valid time among rows known at or before
    /// `known` and valid at or before `valid` (ties on valid time broken by
    /// the greater known time), through the one complete `temporal2`
    /// sidecar serving `columns`. `None` leaves that clock unbounded.
    Bitemporal {
        /// Lookup columns; they must include every key column of the
        /// sidecar.
        columns: Vec<String>,
        /// Canonical key tuples.
        keys: Vec<Vec<u8>>,
        /// Upper bound on valid time, in the valid column's unit.
        valid: Option<i64>,
        /// Upper bound on known time, in the known column's unit.
        known: Option<i64>,
    },
}

/// The answer of [`Planner::count`]: exact, from per-key counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyCount {
    /// The pinned manifest sequence.
    pub manifest_version: String,
    /// Distinct keys counted (a key containing a null matches nothing and
    /// is not counted, ADR 014).
    pub keys: u64,
    /// Rows whose key tuple is one of the keys, in live row groups.
    pub rows: u64,
    /// Live row groups holding at least one of the keys.
    pub row_groups: u64,
    /// Live row groups in the index.
    pub row_groups_total: u64,
    /// Files holding those row groups.
    pub files: u64,
    /// The spec whose counts answered.
    pub counted_by: KeySpec,
}

/// The order of a plan's entries ([`Planner::plan_ordered`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanOrder {
    /// Reading the entries in order, and each entry's row groups in
    /// ascending index, visits the planned row groups in non-decreasing
    /// order of `column`'s row-group minimum (as the range key on `column`
    /// records it; row groups whose every value is null come last).
    Ordered {
        /// The timestamp or date column of the range key.
        column: String,
    },
    /// No such order holds; `reason` says why. Entries are still in a
    /// deterministic order: by each file's earliest row-group minimum when a
    /// timestamp range key exists, else by path.
    Unspecified {
        /// Why, in a sentence.
        reason: String,
    },
}

/// A key spec whose segments exist only in memory, for example a
/// hypothetical spec scored by replay: the raw bytes of each segment.
/// A transient spec is treated as complete; its builder guarantees that its
/// segments cover every live row group of the pinned manifest.
#[derive(Clone, Debug)]
pub struct TransientSpec {
    /// The spec.
    pub spec: KeySpec,
    /// Segment bytes, as core's `EncodedSegment::bytes`.
    pub segments: Vec<Bytes>,
}

/// One segment of a spec taking part in a plan.
enum SegSource {
    Committed { id: Hash, size: u64 },
    Transient(Bytes),
}

struct SpecSegments {
    spec: KeySpec,
    sources: Vec<SegSource>,
}

/// A key lookup's row groups, the rows of its keys and the spec that
/// counted them (when one does), and its non-null probe keys.
type SelectedKeys = (RoaringBitmap, Option<(u64, KeySpec)>, Vec<Vec<u8>>);

/// How a spec serves a lookup (see the module docs).
#[derive(Clone, Debug, PartialEq, Eq)]
enum Serve {
    Project(Vec<usize>),
    Prefix,
}

fn serve(spec: &KeySpec, columns: &[String]) -> Option<Serve> {
    let p = spec.columns();
    let positions: Option<Vec<usize>> = p
        .iter()
        .map(|c| columns.iter().position(|x| x == c))
        .collect();
    if let Some(positions) = positions {
        return Some(Serve::Project(positions));
    }
    let keyed = matches!(spec, KeySpec::Exact { .. } | KeySpec::Temporal { .. });
    (keyed && columns.len() < p.len() && p[..columns.len()] == *columns).then_some(Serve::Prefix)
}

/// Plans lookups against one pinned manifest version (invariant 4).
pub struct Planner<S: StoreExt + ?Sized> {
    snapshot: Snapshot<S>,
    schema: SchemaRef,
    kind: SourceKind,
    policy: FingerprintPolicy,
    parquet: Option<ParquetSource<LocalFs>>,
    locator: Locator,
    verified: Mutex<BTreeSet<usize>>,
    footers: AtomicU64,
}

impl<S: StoreExt + ?Sized> std::fmt::Debug for Planner<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Planner")
            .field("sequence", &self.sequence())
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl<S: StoreExt + ?Sized> Planner<S> {
    /// Pins the current manifest under `index_root`.
    pub fn open(store: Arc<S>, index_root: &str) -> Result<Self> {
        Self::from_snapshot(Snapshot::open(store, index_root)?)
    }

    /// Plans against an already pinned snapshot.
    pub fn from_snapshot(snapshot: Snapshot<S>) -> Result<Self> {
        let m = snapshot.manifest();
        let kind = SourceKind::parse(&m.source.kind)?;
        let policy = parse_policy(m.source.fingerprint_policy.as_deref())?;
        let schema = columns_to_schema(&m.schema.columns)?;
        let locator = Locator::new(&m.rowgroup_space)?;
        let partitioning = Partitioning::parse(m.source.partitioning.as_deref())?;
        let parquet = (kind == SourceKind::Parquet)
            .then(|| parquet_source(Some(&schema), policy, &m.source.root, &partitioning));
        Ok(Self {
            snapshot,
            schema,
            kind,
            policy,
            parquet,
            locator,
            verified: Mutex::new(BTreeSet::new()),
            footers: AtomicU64::new(0),
        })
    }

    /// The pinned manifest's sequence (ADR 020).
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.snapshot.sequence()
    }

    /// The pinned manifest.
    #[must_use]
    pub fn manifest(&self) -> &Manifest {
        self.snapshot.manifest()
    }

    /// The dataset schema the manifest records.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The declared key specs.
    #[must_use]
    pub fn declared_specs(&self) -> &[KeySpec] {
        &self.manifest().schema.key_specs
    }

    /// The sequence at which `spec` became complete, or `None` (see
    /// [`crate::completeness`]).
    #[must_use]
    pub fn completeness(&self, spec: &KeySpec) -> Option<u64> {
        completeness(self.manifest(), spec)
    }

    /// Parquet footers this planner opened to price a plan: zero on a
    /// storage version 2 manifest, whose row-group bytes the plan reads from
    /// the manifest; one per planned file of a version 1 manifest the next
    /// build has not yet filled.
    #[must_use]
    pub fn footers_opened(&self) -> u64 {
        self.footers.load(Ordering::Relaxed)
    }

    /// The sidecars the manifest declares that this binary knows (per-key
    /// time bounds, bitemporal versions).
    #[must_use]
    pub fn declared_sidecars(&self) -> Vec<SidecarDef> {
        declared_sidecars(self.manifest())
    }

    /// The sequence at which sidecar `def` became complete, or `None` (see
    /// [`crate::sidecar_completeness`]).
    #[must_use]
    pub fn sidecar_completeness(&self, def: &SidecarDef) -> Option<u64> {
        sidecar_completeness(self.manifest(), def)
    }

    /// The complete sidecars that take part in planning `lookup`: for a
    /// window, every complete `bounds` sidecar whose key columns the lookup
    /// names and whose time column is the window's; for a bitemporal
    /// lookup, every complete `temporal2` sidecar whose key columns the
    /// lookup names (the plan refuses unless there is exactly one).
    #[must_use]
    pub fn sidecars_for(&self, lookup: &Lookup) -> Vec<SidecarDef> {
        let (kind, columns, time) = match lookup {
            Lookup::Window {
                columns, ts_column, ..
            } => (SidecarKind::Bounds, columns, Some(ts_column)),
            Lookup::Bitemporal { columns, .. } => (SidecarKind::Bitemporal, columns, None),
            _ => return Vec::new(),
        };
        self.declared_sidecars()
            .into_iter()
            .filter(|d| {
                d.kind == kind
                    && d.columns.iter().all(|c| columns.contains(c))
                    && time.is_none_or(|t| d.time_columns.first() == Some(t))
                    && self.sidecar_completeness(d).is_some()
            })
            .collect()
    }

    /// The dataset types of `columns`, which must be distinct schema
    /// columns.
    pub fn column_types(&self, columns: &[String]) -> Result<Vec<(String, DataType)>> {
        if columns.is_empty() {
            return Err(Error::KeySpec("a lookup needs at least one column".into()));
        }
        let mut seen = BTreeSet::new();
        columns
            .iter()
            .map(|c| {
                if !seen.insert(c) {
                    return Err(Error::KeySpec(format!("lookup lists column {c:?} twice")));
                }
                let f = self
                    .schema
                    .field_with_name(c)
                    .map_err(|_| Error::KeySpec(encoding::not_in_schema(&self.schema, c)))?;
                Ok((c.clone(), f.data_type().clone()))
            })
            .collect()
    }

    /// Canonical key tuples over `columns` for every row of `batch`, each
    /// value converted to its column's dataset type (a value that cannot
    /// convert exactly is an error). Rows with a null in any of `columns`
    /// are skipped: a probe containing a null matches nothing (ADR 014).
    /// Sorted, deduplicated.
    pub fn encode_keys(&self, columns: &[String], batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        let types: Vec<DataType> = self
            .column_types(columns)?
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        let cols = columns
            .iter()
            .map(|c| {
                batch
                    .column_by_name(c)
                    .cloned()
                    .ok_or_else(|| Error::KeySpec(format!("column {c:?} not in the key batch")))
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        let keys = KeyColumns::new(&cols, &types)?;
        let mut out = Vec::with_capacity(keys.len());
        let mut buf = Vec::new();
        for row in 0..keys.len() {
            // A null leading column is not encoded; a null elsewhere is
            // encoded as the null marker, which `tuple_has_null` finds.
            if keys.encode_into(row, &mut buf)? && encoding::tuple_has_null(&buf) == Some(false) {
                out.push(buf.clone());
            }
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    /// The canonical tuple of `values` over `columns` at the dataset types.
    pub fn encode_tuple(&self, columns: &[String], values: &[Value<'_>]) -> Result<Vec<u8>> {
        let types: Vec<DataType> = self
            .column_types(columns)?
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        encoding::encode_tuple_as(values, &types)
    }

    /// Plans `lookup` with `specs` (see the module docs).
    pub fn plan(&self, specs: &[KeySpec], lookup: &Lookup) -> Result<ScanPlan> {
        self.plan_with_transient(specs, lookup, &[])
    }

    /// Plans `lookup` with `specs` like [`Planner::plan`], then orders the
    /// plan's entries by a timestamp range key's row-group minimum (see
    /// [`PlanOrder`]). The row groups planned are exactly those of
    /// [`Planner::plan`]; only the order of `entries` differs.
    pub fn plan_ordered(
        &self,
        specs: &[KeySpec],
        lookup: &Lookup,
    ) -> Result<(ScanPlan, PlanOrder)> {
        let mut plan = self.plan(specs, lookup)?;
        let order = self.order(specs, lookup, &mut plan)?;
        Ok((plan, order))
    }

    /// The range spec a plan is ordered by: the one range spec in `specs`
    /// on a timestamp or date column, or, when several are, the one on the
    /// lookup's own time column.
    fn order_spec<'a>(
        &self,
        specs: &'a [KeySpec],
        lookup: &Lookup,
    ) -> std::result::Result<&'a KeySpec, String> {
        let timed: Vec<&KeySpec> = specs
            .iter()
            .filter(|s| match s {
                KeySpec::Range { column } => self.schema.field_with_name(column).is_ok_and(|f| {
                    matches!(
                        f.data_type(),
                        DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
                    )
                }),
                _ => false,
            })
            .collect();
        match timed.as_slice() {
            [] => Err(
                "no complete range key on a timestamp or date column; declare one (for example \
                 range:ts) to get plans in time order"
                    .into(),
            ),
            [one] => Ok(one),
            many => {
                let own: Option<String> = match lookup {
                    Lookup::Between { column, .. } => Some(column.clone()),
                    Lookup::Window { ts_column, .. } => Some(ts_column.clone()),
                    Lookup::Bitemporal { .. } => self
                        .sidecars_for(lookup)
                        .first()
                        .and_then(|d| d.time_columns.first())
                        .cloned(),
                    Lookup::Latest { columns, .. } | Lookup::AsOf { columns, .. } => {
                        specs.iter().find_map(|s| match s {
                            KeySpec::Temporal {
                                columns: c,
                                ts_column,
                                ..
                            } if c.iter().all(|x| columns.contains(x)) => Some(ts_column.clone()),
                            _ => None,
                        })
                    }
                    Lookup::Lookback { columns, .. } => {
                        self.lookback_column(specs, columns).ok().flatten()
                    }
                    Lookup::Keys { .. } => None,
                };
                many.iter()
                    .find(|s| Some(&s.columns()[0]) == own.as_ref())
                    .copied()
                    .ok_or_else(|| {
                        format!(
                            "range keys on several timestamp columns ({}) and the lookup names \
                             none of them, so no one time order applies",
                            many.iter()
                                .map(|s| s.columns()[0].as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })
            }
        }
    }

    /// Orders `plan.entries` by the earliest row-group minimum of the
    /// ordering column among each entry's row groups (row groups with no
    /// minimum, every value null, sort last; ties by path), and says whether
    /// that order is a total time order (see [`PlanOrder`]).
    fn order(&self, specs: &[KeySpec], lookup: &Lookup, plan: &mut ScanPlan) -> Result<PlanOrder> {
        let spec = match self.order_spec(specs, lookup) {
            Ok(s) => s,
            Err(reason) => return Ok(PlanOrder::Unspecified { reason }),
        };
        let column = spec.columns()[0].clone();
        // Every row group's minimum from the range key's own segments.
        let mut mins: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        let name = spec.identity();
        for s in self.manifest().key_spec_segments(&name) {
            let src = SegSource::Committed {
                id: s.id,
                size: s.bytes,
            };
            let r = self.open_segment(spec, &src, true)?;
            for (rg, min, _) in r.range_entries()? {
                let e = mins.entry(rg.0).or_insert_with(|| min.clone());
                if min < *e {
                    *e = min;
                }
            }
        }
        let files = &self.manifest().rowgroup_space.files;
        let by_path: BTreeMap<&str, usize> = files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.fingerprint.path.as_str(), i))
            .collect();
        // Per entry, its row groups' minimums in index order (None: no
        // minimum).
        let mut keyed: Vec<(Vec<Option<Vec<u8>>>, ScanEntry)> = Vec::new();
        for e in std::mem::take(&mut plan.entries) {
            let fi = *by_path.get(e.file.path.as_str()).ok_or_else(|| {
                Error::Other(format!("planned file {} is not tracked", e.file.path))
            })?;
            let ids = &files[fi].rowgroup_ids;
            let rg_mins = e
                .row_groups
                .iter()
                .map(|idx| {
                    let id = usize::try_from(idx)
                        .ok()
                        .and_then(|i| ids.get(i))
                        .ok_or_else(|| {
                            Error::Other(format!("row group {idx} of {} out of range", e.file.path))
                        })?;
                    Ok(mins.get(&id.0).cloned())
                })
                .collect::<Result<Vec<_>>>()?;
            keyed.push((rg_mins, e));
        }
        // None sorts after every Some.
        let rank = |m: &Option<Vec<u8>>| -> Rank { (m.is_none(), m.clone()) };
        keyed.sort_by(|(a, x), (b, y)| {
            let ea = a.iter().map(rank).min();
            let eb = b.iter().map(rank).min();
            ea.cmp(&eb).then_with(|| x.file.path.cmp(&y.file.path))
        });
        let mut previous: Option<(Rank, &str)> = None;
        let mut breach: Option<(String, String)> = None;
        for (rg_mins, e) in &keyed {
            for m in rg_mins {
                let r = rank(m);
                if let Some((p, path)) = &previous {
                    if r < *p && breach.is_none() {
                        breach = Some(((*path).to_string(), e.file.path.clone()));
                    }
                }
                previous = Some((r, e.file.path.as_str()));
            }
        }
        plan.entries = keyed.into_iter().map(|(_, e)| e).collect();
        Ok(match breach {
            None => PlanOrder::Ordered { column },
            Some((a, b)) if a == b => PlanOrder::Unspecified {
                reason: format!(
                    "the row groups of {a} are not in {column} order, so no reading order of \
                     whole files is a time order; entries are ordered by each file's earliest \
                     row group"
                ),
            },
            Some((a, b)) => PlanOrder::Unspecified {
                reason: format!(
                    "row groups of {a} and {b} overlap in {column}, so no reading order of whole \
                     files is a time order; entries are ordered by each file's earliest row group"
                ),
            },
        })
    }

    /// Plans `lookup` with `specs`, where a spec equal to one in `extra`
    /// uses that transient spec's in-memory segments instead of committed
    /// ones.
    pub fn plan_with_transient(
        &self,
        specs: &[KeySpec],
        lookup: &Lookup,
        extra: &[TransientSpec],
    ) -> Result<ScanPlan> {
        let segs = self.resolve(specs, extra)?;
        match lookup {
            Lookup::Keys { columns, keys } => self.plan_keys(&segs, columns, keys),
            Lookup::Latest { columns, keys } => {
                self.plan_temporal(&segs, columns, keys, Versions::Latest)
            }
            Lookup::AsOf { columns, keys, ts } => {
                self.plan_temporal(&segs, columns, keys, Versions::Asof(*ts))
            }
            Lookup::Lookback {
                columns,
                keys,
                lookback,
            } => self.plan_lookback(specs, &segs, columns, keys, *lookback),
            Lookup::Between { column, lo, hi } => self.plan_between(&segs, column, lo, hi),
            Lookup::Window {
                columns,
                keys,
                ts_column,
                lo,
                hi,
            } => {
                let sidecars = self.sidecars_for(lookup);
                self.plan_window(&segs, &sidecars, columns, keys, ts_column, *lo, *hi)
            }
            Lookup::Bitemporal {
                columns,
                keys,
                valid,
                known,
            } => {
                let sidecars = self.sidecars_for(lookup);
                self.plan_bitemporal(&sidecars, columns, keys, *valid, *known)
            }
        }
    }

    /// Counts the rows whose `columns` tuple is one of `keys`
    /// (`count(*) where (columns) in keys`) from the per-key counts of a
    /// complete spec in `specs` that stores them and whose key is exactly
    /// `columns` (`exact:…,counts`, `temporal:…,counts`). Every other spec
    /// in `specs` that serves the lookup narrows the row groups, as in
    /// [`Planner::plan`]. Reads the manifest and the value records of the
    /// keys asked; no data file is opened, not even for a fingerprint.
    /// `None` without such a spec: rows would need a scan.
    pub fn count(
        &self,
        specs: &[KeySpec],
        columns: &[String],
        keys: &[Vec<u8>],
    ) -> Result<Option<KeyCount>> {
        // Only specs that serve the lookup take part; with none, nothing
        // counts.
        let specs: Vec<KeySpec> = specs
            .iter()
            .filter(|s| serve(s, columns).is_some())
            .cloned()
            .collect();
        if specs.is_empty() {
            return Ok(None);
        }
        let segs = self.resolve(&specs, &[])?;
        let (selected, rows, keys) = self.select_keys(&segs, columns, keys)?;
        let Some((rows, counted_by)) = rows else {
            return Ok(None);
        };
        self.locator.check_known(&selected)?;
        let live = &selected & self.locator.live();
        let mut files = BTreeSet::new();
        for id in &live {
            let (fi, _) = self
                .locator
                .locate(RowGroupId(id))
                .ok_or_else(|| Error::Other(format!("live row-group id {id} has no file")))?;
            files.insert(fi);
        }
        Ok(Some(KeyCount {
            manifest_version: self.sequence().to_string(),
            keys: keys.len() as u64,
            rows,
            row_groups: live.len(),
            row_groups_total: self.locator.live().len(),
            files: files.len() as u64,
            counted_by,
        }))
    }

    /// Fills each entry's `columns` and the plan's `projected_bytes` for a
    /// consumer that reads only `columns` (distinct dataset columns): the
    /// compressed bytes of each column over the entry's row groups, from the
    /// manifest's `column_bytes` (storage version 2). A storage version 1
    /// manifest records none, so a Parquet file's are read from its footer;
    /// the result is the number of footers read that way (0 on a version 2
    /// manifest). A text source's chunks have no per-column bytes, so its
    /// entries carry none and count their whole bytes. `plan` must come
    /// from this planner.
    pub fn project(&self, plan: &mut ScanPlan, columns: &[String]) -> Result<u64> {
        if plan.manifest_version != self.sequence().to_string() {
            return Err(Error::Other(format!(
                "the plan is of manifest {} but the planner pins {}",
                plan.manifest_version,
                self.sequence()
            )));
        }
        self.column_types(columns)?;
        let m = self.manifest();
        let width = m.schema.columns.len();
        let positions: Vec<usize> = columns
            .iter()
            .map(|c| {
                m.schema
                    .columns
                    .iter()
                    .position(|d| &d.name == c)
                    .ok_or_else(|| Error::KeySpec(encoding::not_in_schema(&self.schema, c)))
            })
            .collect::<Result<_>>()?;
        let by_path: BTreeMap<&str, usize> = m
            .rowgroup_space
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.fingerprint.path.as_str(), i))
            .collect();
        let mut projected = 0u64;
        let mut from_footers = 0u64;
        for e in &mut plan.entries {
            let fi = *by_path.get(e.file.path.as_str()).ok_or_else(|| {
                Error::Other(format!("{} is not tracked by the manifest", e.file.path))
            })?;
            let f = &m.rowgroup_space.files[fi];
            if self.kind.text_format().is_some() {
                e.columns = None;
                projected += e.bytes;
                continue;
            }
            let footer;
            let column_bytes = match &f.column_bytes {
                Some(b) => b.as_slice(),
                None => {
                    footer = self.footer_column_bytes(fi)?;
                    from_footers += 1;
                    footer.as_slice()
                }
            };
            let mut out = Vec::with_capacity(columns.len());
            for (c, &j) in columns.iter().zip(&positions) {
                let mut bytes = 0u64;
                for idx in &e.row_groups {
                    let i = usize::try_from(idx).unwrap_or(usize::MAX);
                    bytes += i
                        .checked_mul(width)
                        .and_then(|at| column_bytes.get(at.checked_add(j)?))
                        .ok_or_else(|| {
                            Error::Other(format!(
                                "row group {idx} out of range in {}",
                                e.file.path
                            ))
                        })?;
                }
                projected += bytes;
                out.push(ColumnBytes {
                    column: c.clone(),
                    bytes,
                });
            }
            e.columns = Some(out);
        }
        plan.projected_bytes = Some(projected);
        Ok(from_footers)
    }

    /// Compressed bytes of each column chunk of tracked Parquet file `fi`,
    /// row-group-major over the dataset schema's columns (0 for a column
    /// the file does not hold, a virtual partition column), read from its
    /// footer: what a storage version 2 manifest records as
    /// `column_bytes`, for a version 1 manifest that records none.
    fn footer_column_bytes(&self, fi: usize) -> Result<Vec<u64>> {
        let m = self.manifest();
        let f = &m.rowgroup_space.files[fi];
        let parquet = self.parquet.as_ref().ok_or_else(|| {
            Error::Other(format!(
                "{}: no Parquet reader for a Parquet index",
                f.fingerprint.path
            ))
        })?;
        self.footers.fetch_add(1, Ordering::Relaxed);
        let metas = parquet.row_groups(&f.fingerprint)?;
        if metas.len() != f.rowgroup_ids.len() {
            return Err(Error::Source(format!(
                "{}: the footer lists {} row groups, the manifest {}",
                f.fingerprint.path,
                metas.len(),
                f.rowgroup_ids.len()
            )));
        }
        let mut out = Vec::with_capacity(metas.len().saturating_mul(m.schema.columns.len()));
        for rg in &metas {
            for c in &m.schema.columns {
                out.push(
                    rg.stats
                        .iter()
                        .find(|s| s.column == c.name)
                        .and_then(|s| s.bytes)
                        .unwrap_or(0),
                );
            }
        }
        Ok(out)
    }

    // -- specs and segments ------------------------------------------------

    fn resolve(&self, specs: &[KeySpec], extra: &[TransientSpec]) -> Result<Vec<SpecSegments>> {
        let m = self.manifest();
        let mut names = BTreeSet::new();
        specs
            .iter()
            .map(|spec| {
                let name = spec.name();
                if !names.insert(name.clone()) {
                    return Err(Error::KeySpec(format!("key spec {name} is listed twice")));
                }
                if let Some(t) = extra.iter().find(|t| &t.spec == spec) {
                    return Ok(SpecSegments {
                        spec: spec.clone(),
                        sources: t
                            .segments
                            .iter()
                            .cloned()
                            .map(SegSource::Transient)
                            .collect(),
                    });
                }
                if self.completeness(spec).is_none() {
                    return Err(self.not_servable(spec));
                }
                let identity = spec.identity();
                let sources = m
                    .segments
                    .iter()
                    .filter(|s| segment_of(s, &identity))
                    .map(|s| SegSource::Committed {
                        id: s.id,
                        size: s.bytes,
                    })
                    .collect();
                Ok(SpecSegments {
                    spec: spec.clone(),
                    sources,
                })
            })
            .collect()
    }

    fn not_servable(&self, spec: &KeySpec) -> Error {
        let name = spec.name();
        let seq = self.sequence();
        match self.declared_specs().iter().find(|s| s.name() == name) {
            None => Error::KeySpec(format!("key spec {name} is not declared in this index")),
            Some(d) if d != spec => Error::KeySpec(format!(
                "the index declares {} under the name {name}, not {}",
                d.identity(),
                spec.identity()
            )),
            Some(_) => Error::KeySpec(format!(
                "key spec {name} is not complete at manifest sequence {seq} (ADR 012); a query on an \
                 incomplete key spec is an error, never a partial answer"
            )),
        }
    }

    /// Opens one segment against the dataset schema, refusing one recorded
    /// under other key types or indexing another spec. `whole` reads the
    /// whole committed segment (checked against its id) instead of by range.
    fn open_segment(
        &self,
        spec: &KeySpec,
        src: &SegSource,
        whole: bool,
    ) -> Result<SegmentReader<SegmentBytes<'_, S>>> {
        let bytes = match src {
            SegSource::Committed { id, size } if !whole => SegmentBytes::Ranged {
                snapshot: &self.snapshot,
                id: *id,
                size: *size,
            },
            SegSource::Committed { id, .. } => {
                SegmentBytes::Memory(self.snapshot.read_segment(id)?)
            }
            SegSource::Transient(b) => SegmentBytes::Memory(b.clone()),
        };
        let reader = SegmentReader::open_for_dataset(bytes, &self.schema)?;
        if reader.key_spec() != spec {
            return Err(Error::KeySpec(format!(
                "a segment listed under {} indexes {}; the manifest lists it under another \
                 spec, so restore the manifest from manifests/ or rebuild the spec",
                spec.identity(),
                reader.key_spec().identity()
            )));
        }
        Ok(reader)
    }

    fn serving<'a>(
        segs: &'a [SpecSegments],
        columns: &[String],
    ) -> Result<Vec<(&'a SpecSegments, Serve)>> {
        let serving: Vec<_> = segs
            .iter()
            .filter_map(|s| serve(&s.spec, columns).map(|v| (s, v)))
            .collect();
        if !segs.is_empty() && serving.is_empty() {
            return Err(Error::KeySpec(format!(
                "none of the key specs {:?} serves a lookup on {columns:?}",
                segs.iter().map(|s| s.spec.name()).collect::<Vec<_>>()
            )));
        }
        Ok(serving)
    }

    /// For each key, the row groups `seg` may hold it in. With `rows`, the
    /// spec stores counts and its key is the whole lookup tuple: the rows of
    /// every key in live row groups are added to it, read from the same
    /// value records.
    fn per_key(
        &self,
        seg: &SpecSegments,
        how: &Serve,
        keys: &[Vec<u8>],
        spans: &[Vec<std::ops::Range<usize>>],
        mut rows: Option<&mut u64>,
    ) -> Result<Vec<RoaringBitmap>> {
        let probes: Vec<Vec<Vec<u8>>> = keys
            .iter()
            .zip(spans)
            .map(|(k, sp)| match how {
                Serve::Project(pos) => zero_variants(&project(k, sp, pos)),
                Serve::Prefix => zero_variants(k),
            })
            .collect();
        let mut out = vec![RoaringBitmap::new(); keys.len()];
        let keyed = matches!(seg.spec, KeySpec::Exact { .. } | KeySpec::Temporal { .. });
        match (keyed, how) {
            (true, Serve::Project(_)) => {
                let mut owners: BTreeMap<&[u8], Vec<usize>> = BTreeMap::new();
                for (i, ps) in probes.iter().enumerate() {
                    for p in ps {
                        owners.entry(p.as_slice()).or_default().push(i);
                    }
                }
                let all: Vec<&[u8]> = owners.keys().copied().collect();
                let live = self.locator.live();
                // The row groups each probe was counted in so far: two
                // segments counting one key in one row group would count
                // its rows twice.
                let mut counted_in: BTreeMap<&[u8], RoaringBitmap> = BTreeMap::new();
                for src in &seg.sources {
                    let r = self.open_segment(&seg.spec, src, false)?;
                    for (probe, value) in r.get(&all)? {
                        if let Some(total) = rows.as_deref_mut() {
                            let key = owners
                                .get_key_value(probe.as_slice())
                                .map_or(&[][..], |(k, _)| *k);
                            let seen = counted_in.entry(key).or_default();
                            if !(&*seen & &value.bitmap).is_empty() {
                                return Err(overlapping_counts(&seg.spec));
                            }
                            *seen |= &value.bitmap;
                            // Probes are distinct (both zeros of a key are
                            // two stored keys), so no row counts twice.
                            let counted = value.counted().ok_or_else(|| {
                                Error::Other(format!(
                                    "corrupt segment: a segment of {} holds a key without its \
                                     counts",
                                    seg.spec.identity()
                                ))
                            })?;
                            for (rg, n) in counted {
                                if live.contains(rg.0) {
                                    *total = total.checked_add(n).ok_or_else(|| {
                                        Error::Other("row count overflows u64".into())
                                    })?;
                                }
                            }
                        }
                        for &i in owners.get(probe.as_slice()).map_or(&[][..], Vec::as_slice) {
                            out[i] |= &value.bitmap;
                        }
                    }
                }
            }
            (true, Serve::Prefix) => {
                for src in &seg.sources {
                    let r = self.open_segment(&seg.spec, src, false)?;
                    for (i, ps) in probes.iter().enumerate() {
                        for p in ps {
                            out[i] |= r.lookup_prefix(p)?;
                        }
                    }
                }
            }
            (false, Serve::Project(_)) => {
                // Bloom and range segments are scanned per probe; hold one
                // whole segment at a time.
                for src in &seg.sources {
                    let r = self.open_segment(&seg.spec, src, true)?;
                    for (i, ps) in probes.iter().enumerate() {
                        out[i] |= r.lookup(ps)?;
                    }
                }
            }
            (false, Serve::Prefix) => {
                return Err(Error::KeySpec(format!(
                    "{} cannot answer prefix lookups",
                    seg.spec.name()
                )))
            }
        }
        Ok(out)
    }

    /// Intersects per-key row groups across `serving` into `acc`. With
    /// `rows`, the serving spec at `rows.0` counts the rows of the keys
    /// into `rows.1` ([`Self::counting`]).
    fn intersect(
        &self,
        acc: &mut [Option<RoaringBitmap>],
        serving: &[(&SpecSegments, Serve)],
        keys: &[Vec<u8>],
        spans: &[Vec<std::ops::Range<usize>>],
        mut rows: Option<(usize, &mut u64)>,
    ) -> Result<()> {
        for (si, (seg, how)) in serving.iter().enumerate() {
            let count = match rows.as_mut() {
                Some((at, total)) if *at == si => Some(&mut **total),
                _ => None,
            };
            let bitmaps = self.per_key(seg, how, keys, spans, count)?;
            for (a, b) in acc.iter_mut().zip(bitmaps) {
                *a = Some(match a.take() {
                    Some(x) => x & b,
                    None => b,
                });
            }
        }
        Ok(())
    }

    // -- lookups -------------------------------------------------------------

    fn plan_keys(
        &self,
        segs: &[SpecSegments],
        columns: &[String],
        keys: &[Vec<u8>],
    ) -> Result<ScanPlan> {
        let (selected, rows, keys) = self.select_keys(segs, columns, keys)?;
        let residual = Predicate::In {
            columns: columns.to_vec(),
            values: expanded(keys.iter()),
        };
        let mut plan = self.finish(&selected, residual)?;
        plan.rows = rows.map(|(n, _)| Quantity::exact(n));
        Ok(plan)
    }

    /// The row groups of a key lookup, the rows of its keys when a serving
    /// spec counts them ([`Self::counting`]), and the non-null probe keys.
    fn select_keys(
        &self,
        segs: &[SpecSegments],
        columns: &[String],
        keys: &[Vec<u8>],
    ) -> Result<SelectedKeys> {
        let types = self.column_types(columns)?;
        let (keys, spans) = non_null_probes(keys, &types)?;
        let serving = Self::serving(segs, columns)?;
        let counting = Self::counting(&serving, columns);
        let mut rows = counting.map(|_| 0u64);
        let selected = if keys.is_empty() {
            // No key, no row: never a full scan.
            RoaringBitmap::new()
        } else if segs.is_empty() {
            self.locator.live().clone()
        } else {
            let mut acc = vec![None; keys.len()];
            let count = counting.zip(rows.as_mut());
            self.intersect(&mut acc, &serving, &keys, &spans, count)?;
            acc.into_iter()
                .flatten()
                .fold(RoaringBitmap::new(), |u, b| u | b)
        };
        let rows = rows.zip(counting.map(|i| serving[i].0.spec.clone()));
        Ok((selected, rows, keys))
    }

    /// The serving spec that counts a key lookup's rows: the first one that
    /// stores counts (`exact:…,counts`, `temporal:…,counts`) and whose key is
    /// the whole lookup tuple, so each probe is one stored key. A spec over
    /// fewer columns would count rows of other tuples sharing its key.
    fn counting(serving: &[(&SpecSegments, Serve)], columns: &[String]) -> Option<usize> {
        serving.iter().position(|(s, how)| {
            s.spec.counts() && matches!(how, Serve::Project(p) if p.len() == columns.len())
        })
    }

    fn plan_temporal(
        &self,
        segs: &[SpecSegments],
        columns: &[String],
        keys: &[Vec<u8>],
        versions: Versions,
    ) -> Result<ScanPlan> {
        let types = self.column_types(columns)?;
        let (keys, spans) = non_null_probes(keys, &types)?;
        let keys = keys.as_slice();
        let serving = Self::serving(segs, columns)?;
        let mut temporal = serving
            .iter()
            .filter(|(s, _)| matches!(s.spec, KeySpec::Temporal { .. }));
        let (t, how) = match (temporal.next(), temporal.next()) {
            (Some((t, how)), None) => (*t, how),
            (None, _) => {
                return Err(Error::KeySpec(format!(
                    "latest and asof need a temporal key spec serving {columns:?}"
                )))
            }
            (Some(_), Some(_)) => {
                return Err(Error::KeySpec(
                    "more than one temporal key spec serves this lookup; name one".into(),
                ))
            }
        };
        let (Serve::Project(positions), KeySpec::Temporal { ts_column, .. }) = (how, &t.spec)
        else {
            return Err(Error::KeySpec(format!(
                "latest and asof need every column of {} in the lookup",
                t.spec.name()
            )));
        };
        let ts_type = self
            .column_types(std::slice::from_ref(ts_column))?
            .remove(0)
            .1;
        // Every variant of each key's projection onto the temporal spec.
        let variants: Vec<Vec<Vec<u8>>> = keys
            .iter()
            .zip(&spans)
            .map(|(k, sp)| zero_variants(&project(k, sp, positions)))
            .collect();
        let readers = t
            .sources
            .iter()
            .map(|src| self.open_segment(&t.spec, src, false))
            .collect::<Result<Vec<_>>>()?;
        let (selection, mut acc) = match versions {
            Versions::Latest | Versions::Asof(_) => {
                let winners = self.winners(&readers, &variants, versions.asof())?;
                let acc: Vec<Option<RoaringBitmap>> = winners
                    .iter()
                    .map(|w| Some(w.as_ref().map(|h| h.row_groups.clone()).unwrap_or_default()))
                    .collect();
                (Selection::Winners(winners), acc)
            }
            Versions::Lookback(lookback) => {
                let hits = self.chain_lookbacks(&readers, &variants, &ts_type, lookback)?;
                let acc = lookback_rows(&hits);
                (Selection::Lookbacks(hits), acc)
            }
        };
        let others: Vec<(&SpecSegments, Serve)> = serving
            .iter()
            .filter(|(s, _)| !std::ptr::eq(*s, t))
            .map(|(s, h)| (*s, h.clone()))
            .collect();
        self.intersect(&mut acc, &others, keys, &spans, None)?;
        let selected = acc
            .into_iter()
            .flatten()
            .fold(RoaringBitmap::new(), |u, b| u | b);
        let residual = match &selection {
            Selection::Winners(winners) => temporal_residual(
                &t.spec,
                ts_column,
                &ts_type,
                columns,
                keys,
                &variants,
                winners,
                versions.asof(),
            )?,
            Selection::Lookbacks(hits) => lookback_residual(
                t.spec.columns(),
                ts_column,
                &ts_type,
                columns,
                keys,
                &variants,
                hits,
            )?,
        };
        self.finish(&selected, residual)
    }

    /// Each key's index-wide winning version (latest, or at `asof`): every
    /// zero variant of the key's projection is probed, and the variants'
    /// hits fold into one, since they are one key under SQL equality.
    fn winners<R: ReadAt>(
        &self,
        readers: &[SegmentReader<R>],
        variants: &[Vec<Vec<u8>>],
        asof: Option<i64>,
    ) -> Result<Vec<Option<VersionHit>>> {
        let mut probes: Vec<Vec<u8>> = variants.iter().flatten().cloned().collect();
        probes.sort_unstable();
        probes.dedup();
        let hits = self.temporal_hits(readers, &probes, asof)?;
        Ok(variants
            .iter()
            .map(|vs| {
                vs.iter()
                    .filter_map(|v| hits.get(v))
                    .fold(None, |acc: Option<VersionHit>, h| {
                        Some(match acc {
                            Some(mut a) => {
                                a.combine(h);
                                a
                            }
                            None => h.clone(),
                        })
                    })
            })
            .collect())
    }

    /// Each key's lookback window (module docs) from the version chains of
    /// every segment of the spec, counting only live row groups; a chain
    /// naming a row group the manifest never assigned is an error.
    fn chain_lookbacks<R: ReadAt>(
        &self,
        readers: &[SegmentReader<R>],
        variants: &[Vec<Vec<u8>>],
        ts_type: &DataType,
        lookback: i64,
    ) -> Result<Vec<Option<LookbackHit>>> {
        let probes = sorted_probes(variants);
        let chains = temporal::chains(readers, &probes)?;
        for chain in chains.values() {
            let ids: RoaringBitmap = chain.versions().iter().map(|(_, rg)| rg.0).collect();
            self.locator.check_known(&ids)?;
        }
        let floor = encoding::ts_min(ts_type)?;
        let live = self.locator.live();
        Ok(variants
            .iter()
            .map(|vs| {
                temporal::lookback(
                    vs.iter().filter_map(|v| chains.get(v)),
                    live,
                    lookback,
                    floor,
                )
            })
            .collect())
    }

    /// Index-wide newest version per probe, from core's cross-segment
    /// `latest` or `asof` over every segment of the spec (invariant 5),
    /// restricted to live row groups. A winner that lives only in retired
    /// row groups (its file was rewritten) is recomputed from the stored
    /// version chains over live row groups, so a retired version never
    /// becomes the `NotOlderThan` bound of a live key.
    fn temporal_hits<R: ReadAt>(
        &self,
        readers: &[SegmentReader<R>],
        probes: &[Vec<u8>],
        asof: Option<i64>,
    ) -> Result<BTreeMap<Vec<u8>, VersionHit>> {
        let raw = match asof {
            None => temporal::latest(readers, probes)?,
            Some(ts) => temporal::asof(readers, probes, ts)?,
        };
        let live = self.locator.live();
        let mut out = BTreeMap::new();
        let mut redo = Vec::new();
        for (key, mut hit) in raw {
            self.locator.check_known(&hit.row_groups)?;
            hit.row_groups &= live;
            if hit.row_groups.is_empty() {
                redo.push(key);
            } else {
                out.insert(key, hit);
            }
        }
        if redo.is_empty() {
            return Ok(out);
        }
        for r in readers {
            for (key, value) in r.get(&redo)? {
                self.locator.check_known(&value.bitmap)?;
                if let Some(h) = live_winner(&value.chain, live, asof) {
                    match out.get_mut(&key) {
                        Some(acc) => acc.combine(&h),
                        None => {
                            out.insert(key, h);
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    fn plan_between(
        &self,
        segs: &[SpecSegments],
        column: &str,
        lo: &[u8],
        hi: &[u8],
    ) -> Result<ScanPlan> {
        let types = self.column_types(&[column.to_string()])?;
        split_checked(lo, &types)?;
        split_checked(hi, &types)?;
        if lo == [tag::NULL] || hi == [tag::NULL] {
            // A comparison with a null is never true (ADR 014): no row.
            let nothing = Predicate::In {
                columns: vec![column.to_string()],
                values: Vec::new(),
            };
            return self.finish(&RoaringBitmap::new(), nothing);
        }
        let (lo, hi) = widen_zero_bounds(lo, hi);
        let selected = self.column_range(segs, column, &lo, &hi)?;
        if !segs.is_empty() && selected.is_none() {
            return Err(Error::KeySpec(format!(
                "none of the key specs serves a range lookup on {column:?} (a range spec on it, or an \
                 exact or temporal spec led by it)"
            )));
        }
        let selected = if segs.is_empty() {
            self.locator.live().clone()
        } else {
            selected.unwrap_or_default()
        };
        let residual = Predicate::Between {
            column: column.to_string(),
            lo,
            hi,
        };
        self.finish(&selected, residual)
    }

    /// The row groups the specs on `column` place in `[lo, hi]` (canonical
    /// bounds): a range spec on `column`, or an exact or temporal spec led
    /// by it, intersected across specs; `None` when no spec serves.
    fn column_range(
        &self,
        segs: &[SpecSegments],
        column: &str,
        lo: &[u8],
        hi: &[u8],
    ) -> Result<Option<RoaringBitmap>> {
        let mut selected: Option<RoaringBitmap> = None;
        for s in segs {
            let range = match &s.spec {
                KeySpec::Range { column: c } if c == column => true,
                KeySpec::Exact { columns, .. } | KeySpec::Temporal { columns, .. }
                    if columns.first().is_some_and(|c| c == column) =>
                {
                    false
                }
                _ => continue,
            };
            let mut found = RoaringBitmap::new();
            for src in &s.sources {
                let r = self.open_segment(&s.spec, src, range)?;
                found |= if range {
                    r.lookup_range(lo, hi)?
                } else {
                    r.lookup_prefix_range(lo, hi)?
                };
            }
            selected = Some(match selected {
                Some(x) => x & found,
                None => found,
            });
        }
        Ok(selected)
    }

    /// Opens one committed sidecar segment against the dataset schema,
    /// refusing one that records another definition.
    fn open_sidecar(
        &self,
        def: &SidecarDef,
        seg: &SegmentRef,
    ) -> Result<SidecarReader<SegmentBytes<'_, S>>> {
        let bytes = SegmentBytes::Ranged {
            snapshot: &self.snapshot,
            id: seg.id,
            size: seg.bytes,
        };
        let reader = SidecarReader::open_for_dataset(bytes, &self.schema)?;
        if reader.def() != def {
            return Err(Error::KeySpec(format!(
                "a segment listed under {} records the sidecar {}",
                def.name(),
                reader.def().name()
            )));
        }
        Ok(reader)
    }

    /// The committed segments of sidecar `def`.
    fn sidecar_segments<'m>(&'m self, def: &SidecarDef) -> Vec<&'m SegmentRef> {
        let name = def.name();
        self.manifest()
            .segments
            .iter()
            .filter(|s| segment_of(s, &name))
            .collect()
    }

    /// Keys in a time window (see [`Lookup::Window`]).
    #[allow(clippy::too_many_arguments)]
    fn plan_window(
        &self,
        segs: &[SpecSegments],
        sidecars: &[SidecarDef],
        columns: &[String],
        keys: &[Vec<u8>],
        ts_column: &str,
        lo: i64,
        hi: i64,
    ) -> Result<ScanPlan> {
        let types = self.column_types(columns)?;
        let ts_type = self.column_types(&[ts_column.to_string()])?.remove(0).1;
        encoding::check_ts_type(&ts_type)?;
        let (keys, spans) = non_null_probes(keys, &types)?;
        let (lo_enc, hi_enc) = (encode_ts(lo, &ts_type)?, encode_ts(hi, &ts_type)?);
        let residual = Predicate::And {
            all: vec![
                Predicate::In {
                    columns: columns.to_vec(),
                    values: expanded(keys.iter()),
                },
                Predicate::Between {
                    column: ts_column.to_string(),
                    lo: lo_enc.clone(),
                    hi: hi_enc.clone(),
                },
            ],
        };
        if keys.is_empty() || lo > hi {
            // No key, or an empty window: no row, never a full scan.
            return self.finish(&RoaringBitmap::new(), residual);
        }
        let serving: Vec<(&SpecSegments, Serve)> = segs
            .iter()
            .filter_map(|s| serve(&s.spec, columns).map(|v| (s, v)))
            .collect();
        let window = self.column_range(segs, ts_column, &lo_enc, &hi_enc)?;
        let by_path = self.path_window(ts_column, &ts_type, lo, hi)?;
        if !segs.is_empty()
            && serving.is_empty()
            && window.is_none()
            && sidecars.is_empty()
            && by_path.is_none()
        {
            return Err(Error::KeySpec(format!(
                "none of the key specs {:?} serves a lookup on {columns:?} in a window of \
                 {ts_column:?}; declare exact:{}@{ts_column} or a range spec on {ts_column}",
                segs.iter().map(|s| s.spec.name()).collect::<Vec<_>>(),
                columns.join(",")
            )));
        }
        let narrow = |acc: &mut [Option<RoaringBitmap>], with: &RoaringBitmap| {
            for a in acc {
                *a = Some(match a.take() {
                    Some(x) => x & with,
                    None => with.clone(),
                });
            }
        };
        let mut acc: Vec<Option<RoaringBitmap>> = vec![None; keys.len()];
        self.intersect(&mut acc, &serving, &keys, &spans, None)?;
        if let Some(w) = &window {
            narrow(&mut acc, w);
        }
        // Partition or path-template values of the time column: a file's
        // every row has its path's value, so a file outside the window holds
        // no row of it.
        if let Some(p) = &by_path {
            narrow(&mut acc, p);
        }
        // A temporal spec on the window's own time column lists every
        // version of each key with its row group: per key, only the row
        // groups holding a version inside the window stay.
        for (t, how) in &serving {
            let (KeySpec::Temporal { ts_column: tc, .. }, Serve::Project(positions)) =
                (&t.spec, how)
            else {
                continue;
            };
            if tc != ts_column {
                continue;
            }
            let variants = key_variants(&keys, &spans, positions);
            let inside = self.chain_window(t, &variants, lo, hi)?;
            narrow_per_key(&mut acc, &variants, &inside);
        }
        // Each bounds sidecar keeps, per key, exactly the row groups where
        // that key's own non-null times reach into the window. A row group
        // where the key has no non-null time holds no row of the window.
        for def in sidecars {
            let positions: Vec<usize> = def
                .columns
                .iter()
                .map(|c| columns.iter().position(|x| x == c))
                .collect::<Option<_>>()
                .ok_or_else(|| {
                    Error::Other(format!("{} does not serve {columns:?}", def.name()))
                })?;
            let variants = key_variants(&keys, &spans, &positions);
            let inside = self.bounds_window(def, &variants, lo, hi)?;
            narrow_per_key(&mut acc, &variants, &inside);
        }
        let selected = if segs.is_empty() && sidecars.is_empty() {
            match &by_path {
                Some(p) => self.locator.live() & p,
                None => self.locator.live().clone(),
            }
        } else {
            acc.into_iter()
                .flatten()
                .fold(RoaringBitmap::new(), |u, b| u | b)
        };
        self.finish(&selected, residual)
    }

    /// Per probe of `variants`, the row groups where the temporal spec `t`
    /// records a version inside `[lo, hi]` (its version chains, every
    /// segment).
    fn chain_window(
        &self,
        t: &SpecSegments,
        variants: &[Vec<Vec<u8>>],
        lo: i64,
        hi: i64,
    ) -> Result<BTreeMap<Vec<u8>, RoaringBitmap>> {
        let probes = sorted_probes(variants);
        let mut inside: BTreeMap<Vec<u8>, RoaringBitmap> = BTreeMap::new();
        for src in &t.sources {
            let r = self.open_segment(&t.spec, src, false)?;
            for (key, value) in r.get(&probes)? {
                self.locator.check_known(&value.bitmap)?;
                let e = inside.entry(key).or_default();
                for &(ts, rg) in value.chain.versions() {
                    if lo <= ts && ts <= hi {
                        e.insert(rg.0);
                    }
                }
            }
        }
        Ok(inside)
    }

    /// Per probe of `variants`, the row groups where the bounds sidecar
    /// `def` records the key's own time range overlapping `[lo, hi]`.
    fn bounds_window(
        &self,
        def: &SidecarDef,
        variants: &[Vec<Vec<u8>>],
        lo: i64,
        hi: i64,
    ) -> Result<BTreeMap<Vec<u8>, RoaringBitmap>> {
        let probes = sorted_probes(variants);
        let mut inside: BTreeMap<Vec<u8>, RoaringBitmap> = BTreeMap::new();
        for seg in self.sidecar_segments(def) {
            let r = self.open_sidecar(def, seg)?;
            for (key, bounds) in r.bounds(&probes)? {
                let e = inside.entry(key).or_default();
                for b in bounds.iter().filter(|b| b.overlaps(lo, hi)) {
                    e.insert(b.rg.0);
                }
            }
        }
        Ok(inside)
    }

    /// The live row groups whose file's path gives `ts_column` a value in
    /// `[lo, hi]` (in the column's unit), when the manifest's partitioning
    /// makes `ts_column` a column of every file (hive directories or a path
    /// template); `None` when it does not. A null path value is in no
    /// window (ADR 014).
    fn path_window(
        &self,
        ts_column: &str,
        ts_type: &DataType,
        lo: i64,
        hi: i64,
    ) -> Result<Option<RoaringBitmap>> {
        let m = self.manifest();
        let scheme = Partitioning::parse(m.source.partitioning.as_deref())?;
        let files = &m.rowgroup_space.files;
        let Some(inside) = bellwether_source::partition::paths_in_window(
            &scheme,
            &m.source.root,
            files.iter().map(|f| f.fingerprint.path.as_str()),
            ts_column,
            ts_type,
            lo,
            hi,
        )?
        else {
            return Ok(None);
        };
        let mut out = RoaringBitmap::new();
        for (f, _) in files.iter().zip(inside).filter(|(_, i)| *i) {
            out.extend(f.rowgroup_ids.iter().map(|id| id.0));
        }
        out &= self.locator.live();
        Ok(Some(out))
    }

    /// The column whose path values prune `lookup` (see
    /// [`Lookup::Window`]): the window's time column when the manifest's
    /// partitioning makes it a column of every file, else `None`.
    pub fn path_column_for(&self, lookup: &Lookup) -> Result<Option<String>> {
        let Lookup::Window { ts_column, .. } = lookup else {
            return Ok(None);
        };
        let ts_type = self
            .column_types(std::slice::from_ref(ts_column))?
            .remove(0)
            .1;
        Ok(self
            .path_window(ts_column, &ts_type, i64::MIN, i64::MAX)?
            .map(|_| ts_column.clone()))
    }

    /// How a lookback on `columns` reads each key's versions (see
    /// [`Lookup::Lookback`]): the one temporal spec of `specs` whose columns
    /// are all in `columns`, else the one complete `bounds` sidecar whose
    /// key columns are. `None` when neither exists; several of the kind
    /// chosen is an error, since each defines its own key and so its own
    /// windows.
    fn lookback_source(&self, specs: &[KeySpec], columns: &[String]) -> Result<Option<Versioned>> {
        let mut temporal = specs.iter().filter_map(|s| match s {
            KeySpec::Temporal {
                columns: c,
                ts_column,
                ..
            } if c.iter().all(|x| columns.contains(x)) => Some(ts_column),
            _ => None,
        });
        match (temporal.next(), temporal.next()) {
            (Some(ts), None) => return Ok(Some(Versioned::Temporal(ts.clone()))),
            (Some(_), Some(_)) => {
                return Err(Error::KeySpec(
                    "more than one temporal key spec serves this lookup; name one".into(),
                ))
            }
            (None, _) => {}
        }
        let bounds: Vec<SidecarDef> = self
            .declared_sidecars()
            .into_iter()
            .filter(|d| {
                d.kind == SidecarKind::Bounds
                    && d.columns.iter().all(|c| columns.contains(c))
                    && self.sidecar_completeness(d).is_some()
            })
            .collect();
        match <[SidecarDef; 1]>::try_from(bounds) {
            Ok([one]) => Ok(Some(Versioned::Bounds(one))),
            Err(none) if none.is_empty() => Ok(None),
            Err(many) => {
                Err(Error::KeySpec(format!(
                "several per-key bounds ({}) serve a lookback on {columns:?}, each with its own \
                 key and so its own windows; declare a temporal key on the key columns",
                many.iter().map(SidecarDef::name).collect::<Vec<_>>().join(", ")
            )))
            }
        }
    }

    /// The timestamp column a lookback on `columns` is measured on (see
    /// [`Lookup::Lookback`]): the temporal spec's, else the bounds
    /// sidecar's. `None` when no temporal spec or complete bounds sidecar
    /// serves `columns`.
    pub fn lookback_column(&self, specs: &[KeySpec], columns: &[String]) -> Result<Option<String>> {
        Ok(self.lookback_source(specs, columns)?.map(|v| match v {
            Versioned::Temporal(ts) => ts,
            Versioned::Bounds(def) => def.time_columns[0].clone(),
        }))
    }

    /// The time column of a window lookup on `columns` when none is named
    /// ([`default_window_column`] over this index: the time columns of its
    /// complete per-key bounds sidecars on `columns`, `specs`, and the
    /// manifest's partition columns). `None` when no level has one.
    pub fn window_column(&self, specs: &[KeySpec], columns: &[String]) -> Result<Option<String>> {
        let bounded = self
            .declared_sidecars()
            .into_iter()
            .filter(|d| {
                d.kind == SidecarKind::Bounds
                    && d.columns.iter().all(|c| columns.contains(c))
                    && self.sidecar_completeness(d).is_some()
            })
            .map(|d| d.time_columns[0].clone())
            .collect();
        let m = self.manifest();
        let scheme = Partitioning::parse(m.source.partitioning.as_deref())?;
        let first = m
            .rowgroup_space
            .files
            .first()
            .map(|f| f.fingerprint.path.as_str());
        let partition = partition_columns(&scheme, &m.source.root, first)?;
        default_window_column(&self.schema, specs, bounded, partition, columns)
    }

    /// Each key's lookback window (see [`Lookup::Lookback`]).
    fn plan_lookback(
        &self,
        specs: &[KeySpec],
        segs: &[SpecSegments],
        columns: &[String],
        keys: &[Vec<u8>],
        lookback: i64,
    ) -> Result<ScanPlan> {
        if lookback < 0 {
            return Err(Error::KeySpec(format!(
                "a lookback window cannot be negative ({lookback})"
            )));
        }
        match self.lookback_source(specs, columns)? {
            Some(Versioned::Temporal(_)) => {
                self.plan_temporal(segs, columns, keys, Versions::Lookback(lookback))
            }
            Some(Versioned::Bounds(def)) => {
                self.plan_bounds_lookback(segs, &def, columns, keys, lookback)
            }
            None => Err(Error::KeySpec(format!(
                "a lookback needs each key's newest time: declare a temporal key \
                 `temporal:{c}@<ts>` or per-key bounds `exact:{c}@<ts>`",
                c = columns.join(",")
            ))),
        }
    }

    /// Each key's lookback window from the bounds sidecar `def`: the key's
    /// newest time is the greatest of its own per-row-group maxima over live
    /// row groups, and its window keeps the live row groups where its own
    /// time range reaches into `[newest − lookback, newest]`. Every serving
    /// key spec narrows each key's row groups, as for any lookup.
    fn plan_bounds_lookback(
        &self,
        segs: &[SpecSegments],
        def: &SidecarDef,
        columns: &[String],
        keys: &[Vec<u8>],
        lookback: i64,
    ) -> Result<ScanPlan> {
        let types = self.column_types(columns)?;
        let (keys, spans) = non_null_probes(keys, &types)?;
        let ts_column = &def.time_columns[0];
        let ts_type = self
            .column_types(std::slice::from_ref(ts_column))?
            .remove(0)
            .1;
        let floor = encoding::ts_min(&ts_type)?;
        let positions: Vec<usize> = def
            .columns
            .iter()
            .map(|c| columns.iter().position(|x| x == c))
            .collect::<Option<_>>()
            .ok_or_else(|| Error::Other(format!("{} does not serve {columns:?}", def.name())))?;
        let variants = key_variants(&keys, &spans, &positions);
        let probes = sorted_probes(&variants);
        let live = self.locator.live();
        let mut ranges: BTreeMap<Vec<u8>, Vec<RgBounds>> = BTreeMap::new();
        for seg in self.sidecar_segments(def) {
            let r = self.open_sidecar(def, seg)?;
            for (key, bounds) in r.bounds(&probes)? {
                ranges
                    .entry(key)
                    .or_default()
                    .extend(bounds.into_iter().filter(|b| live.contains(b.rg.0)));
            }
        }
        let hits: Vec<Option<LookbackHit>> = variants
            .iter()
            .map(|vs| {
                let mine = || vs.iter().filter_map(|v| ranges.get(v)).flatten();
                let hi = mine().map(|b| b.max).max()?;
                let lo = hi.saturating_sub(lookback).max(floor);
                Some(LookbackHit {
                    lo,
                    hi,
                    row_groups: mine()
                        .filter(|b| b.overlaps(lo, hi))
                        .map(|b| b.rg.0)
                        .collect(),
                })
            })
            .collect();
        // The sidecar bounds every key, so key specs that serve none of the
        // lookup's columns only take no part.
        let serving: Vec<(&SpecSegments, Serve)> = segs
            .iter()
            .filter_map(|s| serve(&s.spec, columns).map(|v| (s, v)))
            .collect();
        let mut acc = lookback_rows(&hits);
        self.intersect(&mut acc, &serving, &keys, &spans, None)?;
        let selected = acc
            .into_iter()
            .flatten()
            .fold(RoaringBitmap::new(), |u, b| u | b);
        let residual = lookback_residual(
            &def.columns,
            ts_column,
            &ts_type,
            columns,
            &keys,
            &variants,
            &hits,
        )?;
        self.finish(&selected, residual)
    }

    /// The bitemporal as-of (see [`Lookup::Bitemporal`]).
    fn plan_bitemporal(
        &self,
        sidecars: &[SidecarDef],
        columns: &[String],
        keys: &[Vec<u8>],
        valid: Option<i64>,
        known: Option<i64>,
    ) -> Result<ScanPlan> {
        let def = match sidecars {
            [one] => one,
            [] => {
                return Err(Error::KeySpec(format!(
                    "a bitemporal lookup needs a complete temporal2 sidecar on key columns the \
                     lookup names ({}); declare temporal2:<key>@<valid>,<known> with \
                     `bellwether index --key`",
                    columns.join(", ")
                )))
            }
            many => {
                return Err(Error::KeySpec(format!(
                    "more than one temporal2 sidecar serves a lookup on {}: {}; look up with \
                     exactly the key columns of one",
                    columns.join(", "),
                    many.iter()
                        .map(SidecarDef::name)
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
            }
        };
        let types = self.column_types(columns)?;
        let (keys, spans) = non_null_probes(keys, &types)?;
        let (valid_col, known_col) = match def.time_columns.as_slice() {
            [v, k] => (v.clone(), k.clone()),
            _ => {
                return Err(Error::Other(format!(
                    "{} has no two time columns",
                    def.name()
                )))
            }
        };
        let valid_type = self
            .column_types(std::slice::from_ref(&valid_col))?
            .remove(0)
            .1;
        let known_type = self
            .column_types(std::slice::from_ref(&known_col))?
            .remove(0)
            .1;
        let positions: Vec<usize> = def
            .columns
            .iter()
            .map(|c| columns.iter().position(|x| x == c))
            .collect::<Option<_>>()
            .ok_or_else(|| Error::Other(format!("{} does not serve {columns:?}", def.name())))?;
        let variants: Vec<Vec<Vec<u8>>> = keys
            .iter()
            .zip(&spans)
            .map(|(k, sp)| zero_variants(&project(k, sp, &positions)))
            .collect();
        let mut probes: Vec<Vec<u8>> = variants.iter().flatten().cloned().collect();
        probes.sort_unstable();
        probes.dedup();
        // Every live version of every probe, from every segment
        // (index-wide, so a winner in a row group another spec would prune
        // still bounds the residual; invariant 5).
        let live = self.locator.live();
        let mut triples: BTreeMap<Vec<u8>, Vec<Triple>> = BTreeMap::new();
        for seg in self.sidecar_segments(def) {
            let r = self.open_sidecar(def, seg)?;
            for (key, ts) in r.triples(&probes)? {
                let ids: RoaringBitmap = ts.iter().map(|t| t.rg.0).collect();
                self.locator.check_known(&ids)?;
                triples
                    .entry(key)
                    .or_default()
                    .extend(ts.into_iter().filter(|t| live.contains(t.rg.0)));
            }
        }
        let winners: Vec<Option<BitemporalHit>> = variants
            .iter()
            .map(|vs| {
                bitemporal_winner(
                    vs.iter().filter_map(|v| triples.get(v)).flatten(),
                    valid,
                    known,
                )
            })
            .collect();
        let selected = winners
            .iter()
            .flatten()
            .fold(RoaringBitmap::new(), |u, h| u | &h.row_groups);
        let residual = bitemporal_residual(
            def,
            columns,
            &keys,
            &variants,
            &winners,
            &Clock {
                column: valid_col,
                dtype: valid_type,
                bound: valid,
            },
            &Clock {
                column: known_col,
                dtype: known_type,
                bound: known,
            },
        )?;
        self.finish(&selected, residual)
    }

    // -- the plan --------------------------------------------------------------

    fn finish(&self, selected: &RoaringBitmap, residual: Predicate) -> Result<ScanPlan> {
        self.locator.check_known(selected)?;
        let live = selected & self.locator.live();
        let mut per_file: BTreeMap<usize, RoaringBitmap> = BTreeMap::new();
        for id in &live {
            let (fi, idx) = self
                .locator
                .locate(RowGroupId(id))
                .ok_or_else(|| Error::Other(format!("live row-group id {id} has no file")))?;
            per_file.entry(fi).or_default().insert(idx);
        }
        let files = &self.manifest().rowgroup_space.files;
        let mut entries = Vec::with_capacity(per_file.len());
        for (fi, row_groups) in per_file {
            let sizes = self.row_group_bytes(fi)?;
            let mut bytes = 0u64;
            for idx in &row_groups {
                let size = usize::try_from(idx)
                    .ok()
                    .and_then(|i| sizes.get(i))
                    .ok_or_else(|| Error::Other(format!("row group {idx} out of range")))?;
                bytes += size;
            }
            entries.push(ScanEntry {
                file: files[fi].fingerprint.clone(),
                row_groups,
                bytes,
                columns: None,
            });
        }
        entries.sort_by(|a, b| a.file.path.cmp(&b.file.path));
        Ok(ScanPlan {
            manifest_version: self.sequence().to_string(),
            bytes: entries.iter().map(|e| e.bytes).sum(),
            row_groups_selected: entries.iter().map(|e| e.row_groups.len()).sum(),
            row_groups_total: self.locator.live().len(),
            entries,
            residual: Some(residual),
            projected_bytes: None,
            order: None,
            rows: None,
        })
    }

    /// Compressed bytes of each row group of tracked file `fi`, by
    /// `index_in_file`: the manifest's `rowgroup_bytes` (storage version
    /// 2), else the Parquet footer's row-group sizes or the recorded chunk
    /// lengths (a version 1 manifest the next build has not yet filled).
    /// Every path verifies the file's fingerprint; only the version 1
    /// Parquet path opens a footer.
    fn row_group_bytes(&self, fi: usize) -> Result<std::borrow::Cow<'_, [u64]>> {
        let f = &self.manifest().rowgroup_space.files[fi];
        let path = &f.fingerprint.path;
        // `Manifest::from_slice` refused misaligned arrays.
        if let Some(bytes) = &f.rowgroup_bytes {
            self.check_fingerprint(fi)?;
            return Ok(std::borrow::Cow::Borrowed(bytes));
        }
        if let Some(parquet) = &self.parquet {
            self.footers.fetch_add(1, Ordering::Relaxed);
            let metas = parquet.row_groups(&f.fingerprint)?;
            if metas.len() != f.rowgroup_ids.len() {
                return Err(Error::Source(format!(
                    "{path}: the footer lists {} row groups, the manifest {}",
                    metas.len(),
                    f.rowgroup_ids.len()
                )));
            }
            return Ok(std::borrow::Cow::Owned(
                metas.iter().map(|m| m.bytes).collect(),
            ));
        }
        let chunks = f
            .chunks
            .as_ref()
            .filter(|c| c.len() == f.rowgroup_ids.len())
            .ok_or_else(|| Error::Other(format!("{path}: a text file entry without its chunks")))?;
        self.check_fingerprint(fi)?;
        Ok(std::borrow::Cow::Owned(
            chunks
                .iter()
                .map(|c| c.end.saturating_sub(c.start))
                .collect(),
        ))
    }

    /// Verifies tracked file `fi` against its recorded fingerprint, once
    /// per planner, under the index's policy (metadata and a bounded hash;
    /// no footer is parsed).
    fn check_fingerprint(&self, fi: usize) -> Result<()> {
        let f = &self.manifest().rowgroup_space.files[fi];
        let checked = self
            .verified
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&fi);
        if checked {
            return Ok(());
        }
        let now = fingerprint_with_policy(&LocalFs, &f.fingerprint.path, self.policy)?;
        if now != f.fingerprint {
            return Err(Error::Source(format!(
                "{}: fingerprint drift since manifest sequence {}; run a build",
                f.fingerprint.path,
                self.sequence()
            )));
        }
        self.verified
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(fi);
        Ok(())
    }
}

/// The partition columns `scheme` gives the files under `root`: a
/// template's placeholders, or the `name=value` directories of `first`, one
/// file's path (every file has the same ones).
pub fn partition_columns(
    scheme: &Partitioning,
    root: &str,
    first: Option<&str>,
) -> Result<Vec<String>> {
    Ok(match (scheme, first) {
        (Partitioning::Template(t), _) => t.names().map(str::to_string).collect(),
        (Partitioning::Hive, Some(path)) => scheme
            .values(root, path)?
            .into_iter()
            .map(|v| v.name)
            .collect(),
        _ => Vec::new(),
    })
}

/// The time column of a window on `columns` when none is named: the one
/// time column of the complete per-key bounds sidecars on those columns
/// (`bounded`), else of the range specs on a timestamp or date column,
/// else of the temporal specs on those columns, else of the partition
/// columns (hive directories or path-template placeholders) of a timestamp
/// or date type. Several at the first level that has any is an error asking
/// for `--ts`; `None` when no level has one.
pub fn default_window_column(
    schema: &arrow::datatypes::Schema,
    specs: &[KeySpec],
    bounded: Vec<String>,
    partition: Vec<String>,
    columns: &[String],
) -> Result<Option<String>> {
    let is_time = |c: &String| {
        schema.field_with_name(c).is_ok_and(|f| {
            matches!(
                f.data_type(),
                DataType::Timestamp(..) | DataType::Date32 | DataType::Date64
            )
        })
    };
    let ranged: Vec<String> = specs
        .iter()
        .filter_map(|s| match s {
            KeySpec::Range { column } => Some(column.clone()),
            _ => None,
        })
        .collect();
    let temporal: Vec<String> = specs
        .iter()
        .filter_map(|s| match s {
            KeySpec::Temporal {
                columns: c,
                ts_column,
                ..
            } if c.iter().all(|x| columns.contains(x)) => Some(ts_column.clone()),
            _ => None,
        })
        .collect();
    let levels = [
        ("per-key bounds", bounded),
        ("range keys", ranged),
        ("temporal keys", temporal),
        ("partition columns", partition),
    ];
    for (what, found) in levels {
        let mut found: Vec<String> = found.into_iter().filter(|c| is_time(c)).collect();
        found.sort();
        found.dedup();
        match found.as_slice() {
            [] => {}
            [one] => return Ok(Some(one.clone())),
            many => {
                return Err(Error::KeySpec(format!(
                    "{what} exist on several time columns ({}); name one with --ts",
                    many.join(", ")
                )))
            }
        }
    }
    Ok(None)
}

/// The order key of a row-group minimum: `None` (every value null) last.
type Rank = (bool, Option<Vec<u8>>);

/// One clock of a bitemporal lookup: its column, dataset type, and bound.
struct Clock {
    column: String,
    dtype: DataType,
    bound: Option<i64>,
}

/// The residual of a bitemporal lookup (`docs/format.md`, "Sidecar
/// kinds"): `In` over keys with a winner, both clocks bounded, then
/// `NotOlderThan` on valid and on known per key, then `LatestPerKey`.
fn bitemporal_residual(
    def: &SidecarDef,
    columns: &[String],
    keys: &[Vec<u8>],
    variants: &[Vec<Vec<u8>>],
    winners: &[Option<BitemporalHit>],
    valid: &Clock,
    known: &Clock,
) -> Result<Predicate> {
    let with_version = keys
        .iter()
        .zip(winners)
        .filter(|(_, w)| w.is_some())
        .map(|(k, _)| k);
    let mut all = vec![Predicate::In {
        columns: columns.to_vec(),
        values: expanded(with_version),
    }];
    let hits: Vec<&BitemporalHit> = winners.iter().flatten().collect();
    if let (Some(lo_v), Some(hi_v), Some(lo_k), Some(hi_k)) = (
        hits.iter().map(|h| h.valid).min(),
        hits.iter().map(|h| h.valid).max(),
        hits.iter().map(|h| h.known).min(),
        hits.iter().map(|h| h.known).max(),
    ) {
        // Rows valid after `valid`, or known after `known`, never qualify;
        // without a bound, no qualifying row lies beyond the newest winner.
        all.push(Predicate::Between {
            column: valid.column.clone(),
            lo: encode_ts(lo_v, &valid.dtype)?,
            hi: encode_ts(valid.bound.unwrap_or(hi_v), &valid.dtype)?,
        });
        all.push(Predicate::Between {
            column: known.column.clone(),
            lo: encode_ts(lo_k, &known.dtype)?,
            hi: encode_ts(known.bound.unwrap_or(hi_k), &known.dtype)?,
        });
    }
    let mut valid_bounds: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut known_bounds: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (vs, w) in variants.iter().zip(winners) {
        let Some(h) = w else { continue };
        let (v, k) = (
            encode_ts(h.valid, &valid.dtype)?,
            encode_ts(h.known, &known.dtype)?,
        );
        for x in vs {
            valid_bounds.insert(x.clone(), v.clone());
            known_bounds.insert(x.clone(), k.clone());
        }
    }
    // Filters first, then the newest valid time per key, then the newest
    // known time among rows at that valid time (contract-v3 entry 11).
    all.push(Predicate::NotOlderThan {
        columns: def.columns.clone(),
        ts_column: valid.column.clone(),
        bounds: valid_bounds.into_iter().collect(),
    });
    all.push(Predicate::NotOlderThan {
        columns: def.columns.clone(),
        ts_column: known.column.clone(),
        bounds: known_bounds.into_iter().collect(),
    });
    all.push(Predicate::LatestPerKey {
        columns: def.columns.clone(),
        ts_column: valid.column.clone(),
    });
    Ok(Predicate::And { all })
}

/// Probes and, for each, the byte spans of its elements.
type Probes = (Vec<Vec<u8>>, Vec<Vec<std::ops::Range<usize>>>);

/// The probes of `keys` that contain no null element, each with its
/// element spans, after checking every probe against the lookup columns'
/// dataset types. A probe containing a null matches nothing (ADR 014), so
/// it is dropped before any lookup, prefix or point, and from the
/// residual.
fn non_null_probes(keys: &[Vec<u8>], types: &[(String, DataType)]) -> Result<Probes> {
    let mut kept = (
        Vec::with_capacity(keys.len()),
        Vec::with_capacity(keys.len()),
    );
    for key in keys {
        let spans = split_checked(key, types)?;
        if spans.iter().all(|sp| key[sp.start] != tag::NULL) {
            kept.0.push(key.clone());
            kept.1.push(spans);
        }
    }
    Ok(kept)
}

/// The refusal for two segments of one counting spec that both count a
/// key in one row group. No build commits that (each row group is indexed
/// by one segment of a spec, and merge refuses overlapping counts), so the
/// manifest was written by another tool or edited; summing would count the
/// rows twice.
#[must_use]
pub fn overlapping_counts(spec: &KeySpec) -> Error {
    Error::Other(format!(
        "corrupt index: two segments listed under {} count the same key in the same row group, \
         which no build commits, so its rows would be counted twice; restore the manifest from \
         manifests/ or rebuild the spec with `bellwether index`",
        spec.identity()
    ))
}

/// Every zero variant of every key, sorted and deduplicated.
fn expanded<'a>(keys: impl Iterator<Item = &'a Vec<u8>>) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = keys.flat_map(|k| zero_variants(k)).collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// Every zero variant of each key's projection onto `positions`.
fn key_variants(
    keys: &[Vec<u8>],
    spans: &[Vec<std::ops::Range<usize>>],
    positions: &[usize],
) -> Vec<Vec<Vec<u8>>> {
    keys.iter()
        .zip(spans)
        .map(|(k, sp)| zero_variants(&project(k, sp, positions)))
        .collect()
}

/// Every variant of `variants`, sorted and deduplicated.
fn sorted_probes(variants: &[Vec<Vec<u8>>]) -> Vec<Vec<u8>> {
    let mut probes: Vec<Vec<u8>> = variants.iter().flatten().cloned().collect();
    probes.sort_unstable();
    probes.dedup();
    probes
}

/// Narrows each key's row groups in `acc` to the union of what `inside`
/// holds for its variants (variants are one key under SQL equality).
fn narrow_per_key(
    acc: &mut [Option<RoaringBitmap>],
    variants: &[Vec<Vec<u8>>],
    inside: &BTreeMap<Vec<u8>, RoaringBitmap>,
) {
    for (a, vs) in acc.iter_mut().zip(variants) {
        let mine = vs
            .iter()
            .filter_map(|v| inside.get(v))
            .fold(RoaringBitmap::new(), |u, b| u | b);
        *a = Some(match a.take() {
            Some(x) => x & mine,
            None => mine,
        });
    }
}

/// The newest version in `chain` held by a live row group (and, for asof,
/// at or before `asof`).
fn live_winner(
    chain: &VersionChain,
    live: &RoaringBitmap,
    asof: Option<i64>,
) -> Option<VersionHit> {
    let mut best: Option<VersionHit> = None;
    for &(ts, rg) in chain.versions() {
        if !live.contains(rg.0) || asof.is_some_and(|a| ts > a) {
            continue;
        }
        let hit = VersionHit {
            ts,
            row_groups: std::iter::once(rg.0).collect(),
        };
        match &mut best {
            Some(b) => b.combine(&hit),
            None => best = Some(hit),
        }
    }
    best
}

/// Which versions of each key a temporal lookup asks for.
#[derive(Clone, Copy)]
enum Versions {
    Latest,
    Asof(i64),
    /// Every version in the key's own lookback window of this length.
    Lookback(i64),
}

impl Versions {
    fn asof(self) -> Option<i64> {
        match self {
            Self::Asof(ts) => Some(ts),
            Self::Latest | Self::Lookback(_) => None,
        }
    }
}

/// Where a lookback reads each key's versions (see [`Lookup::Lookback`]).
enum Versioned {
    /// The temporal spec on this timestamp column: its version chains.
    Temporal(String),
    /// This `bounds` sidecar: each key's own time range per row group.
    Bounds(SidecarDef),
}

/// Per key, what a temporal lookup selected: its winning version, or its
/// lookback window.
enum Selection {
    Winners(Vec<Option<VersionHit>>),
    Lookbacks(Vec<Option<LookbackHit>>),
}

/// Each key's row groups in its lookback window (none without a window).
fn lookback_rows(hits: &[Option<LookbackHit>]) -> Vec<Option<RoaringBitmap>> {
    hits.iter()
        .map(|w| Some(w.as_ref().map(|h| h.row_groups.clone()).unwrap_or_default()))
        .collect()
}

/// The residual of a lookback (see the module docs): `In` over the keys
/// with a window, then `NotOlderThan` with each key's window start over
/// `key_columns`, the columns the windows are keyed on. Each window ends at
/// the key's newest live version, so only the lower bound needs stating.
fn lookback_residual(
    key_columns: &[String],
    ts_column: &str,
    ts_type: &DataType,
    columns: &[String],
    keys: &[Vec<u8>],
    variants: &[Vec<Vec<u8>>],
    hits: &[Option<LookbackHit>],
) -> Result<Predicate> {
    let with_window = keys
        .iter()
        .zip(hits)
        .filter(|(_, w)| w.is_some())
        .map(|(k, _)| k);
    let mut bounds: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (vs, w) in variants.iter().zip(hits) {
        let Some(w) = w else { continue };
        let lo = encode_ts(w.lo, ts_type)?;
        for v in vs {
            bounds.insert(v.clone(), lo.clone());
        }
    }
    Ok(Predicate::And {
        all: vec![
            Predicate::In {
                columns: columns.to_vec(),
                values: expanded(with_window),
            },
            Predicate::NotOlderThan {
                columns: key_columns.to_vec(),
                ts_column: ts_column.to_string(),
                bounds: bounds.into_iter().collect(),
            },
        ],
    })
}

/// The residual of a latest or asof lookup (see the module docs).
#[allow(clippy::too_many_arguments)]
fn temporal_residual(
    spec: &KeySpec,
    ts_column: &str,
    ts_type: &DataType,
    columns: &[String],
    keys: &[Vec<u8>],
    variants: &[Vec<Vec<u8>>],
    winners: &[Option<VersionHit>],
    asof: Option<i64>,
) -> Result<Predicate> {
    let with_version = || {
        keys.iter()
            .zip(winners)
            .filter(|(_, w)| w.is_some())
            .map(|(k, _)| k)
    };
    let mut bounds: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut lowest: Option<i64> = None;
    for (vs, w) in variants.iter().zip(winners) {
        let Some(h) = w else { continue };
        lowest = Some(lowest.map_or(h.ts, |l| l.min(h.ts)));
        let ts = encode_ts(h.ts, ts_type)?;
        for v in vs {
            bounds.insert(v.clone(), ts.clone());
        }
    }
    let mut all = vec![Predicate::In {
        columns: columns.to_vec(),
        values: expanded(with_version()),
    }];
    if let (Some(at), Some(lo)) = (asof, lowest) {
        all.push(Predicate::Between {
            column: ts_column.to_string(),
            lo: encode_ts(lo, ts_type)?,
            hi: encode_ts(at, ts_type)?,
        });
    }
    all.push(Predicate::NotOlderThan {
        columns: spec.columns().to_vec(),
        ts_column: ts_column.to_string(),
        bounds: bounds.into_iter().collect(),
    });
    all.push(Predicate::LatestPerKey {
        columns: spec.columns().to_vec(),
        ts_column: ts_column.to_string(),
    });
    Ok(Predicate::And { all })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(c: &[&str]) -> Vec<String> {
        c.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn serving_rules() {
        let exact_a = KeySpec::Exact {
            columns: cols(&["a"]),
            counts: false,
            bounds: None,
        };
        let exact_ab = KeySpec::Exact {
            columns: cols(&["a", "b"]),
            counts: false,
            bounds: None,
        };
        let bloom_ba = KeySpec::Bloom {
            columns: cols(&["b", "a"]),
            fpr: 0.01,
            bounds: None,
        };
        let range_b = KeySpec::Range { column: "b".into() };
        // projection: the spec's columns are all in the lookup
        assert_eq!(
            serve(&exact_a, &cols(&["a", "b"])),
            Some(Serve::Project(vec![0]))
        );
        assert_eq!(
            serve(&bloom_ba, &cols(&["a", "b"])),
            Some(Serve::Project(vec![1, 0]))
        );
        assert_eq!(
            serve(&range_b, &cols(&["a", "b"])),
            Some(Serve::Project(vec![1]))
        );
        // prefix: the lookup is a strict prefix of an exact spec
        assert_eq!(serve(&exact_ab, &cols(&["a"])), Some(Serve::Prefix));
        // a bloom spec cannot seek a prefix; a range spec on another column
        // does not serve
        assert_eq!(serve(&bloom_ba, &cols(&["b"])), None);
        assert_eq!(serve(&range_b, &cols(&["a"])), None);
        // not a prefix
        assert_eq!(serve(&exact_ab, &cols(&["b"])), None);
    }

    #[test]
    fn live_winner_skips_retired_and_future_versions() {
        let chain = VersionChain::from_versions(vec![
            (1, RowGroupId(0)),
            (5, RowGroupId(1)),
            (5, RowGroupId(2)),
            (9, RowGroupId(3)),
        ]);
        let live: RoaringBitmap = [0u32, 1, 2].into_iter().collect();
        let h = live_winner(&chain, &live, None).unwrap();
        assert_eq!(
            (h.ts, h.row_groups.iter().collect::<Vec<_>>()),
            (5, vec![1, 2])
        );
        let h = live_winner(&chain, &live, Some(4)).unwrap();
        assert_eq!(h.ts, 1);
        assert!(live_winner(&chain, &RoaringBitmap::new(), None).is_none());
    }
}
