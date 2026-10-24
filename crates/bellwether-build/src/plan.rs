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
//! - range lookups: `Between`, widened for signed zero.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef};
use bellwether_core::encoding::{self, tag, KeyColumns, Value};
use bellwether_core::segment::ReadAt;
use bellwether_core::temporal::{self, VersionChain, VersionHit};
use bellwether_core::SegmentReader;
use bellwether_source::{
    fingerprint_with_policy, FingerprintPolicy, LocalFs, ParquetSource, Partitioning,
};
use bellwether_store::{Manifest, Snapshot, StoreExt};
use bellwether_types::{
    Error, Hash, KeySpec, Predicate, Result, RowGroupId, ScanEntry, ScanPlan, Source,
};
use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::config::{columns_to_schema, parse_policy, SourceKind};
use crate::coverage::{completeness, Locator};
use crate::keys::{encode_ts, project, split_checked, widen_zero_bounds, zero_variants};
use crate::seg_io::SegmentBytes;
use crate::sources::parquet_source;

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
        let partitions = (partitioning == Partitioning::Hive).then_some(m.source.root.as_str());
        let parquet = (kind == SourceKind::Parquet)
            .then(|| parquet_source(Some(&schema), policy, partitions));
        Ok(Self {
            snapshot,
            schema,
            kind,
            policy,
            parquet,
            locator,
            verified: Mutex::new(BTreeSet::new()),
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
                let f = self.schema.field_with_name(c).map_err(|_| {
                    Error::KeySpec(format!("column {c:?} is not in the dataset schema"))
                })?;
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
            Lookup::Latest { columns, keys } => self.plan_temporal(&segs, columns, keys, None),
            Lookup::AsOf { columns, keys, ts } => {
                self.plan_temporal(&segs, columns, keys, Some(*ts))
            }
            Lookup::Between { column, lo, hi } => self.plan_between(&segs, column, lo, hi),
        }
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
                let sources = m
                    .segments
                    .iter()
                    .filter(|s| s.key_spec == name)
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
                "the index declares {d:?} under the name {name}, not {spec:?}"
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
                "a segment listed under {} indexes {:?}",
                spec.name(),
                reader.key_spec()
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

    /// For each key, the row groups `seg` may hold it in.
    fn per_key(
        &self,
        seg: &SpecSegments,
        how: &Serve,
        keys: &[Vec<u8>],
        spans: &[Vec<std::ops::Range<usize>>],
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
                for src in &seg.sources {
                    let r = self.open_segment(&seg.spec, src, false)?;
                    for (probe, value) in r.get(&all)? {
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

    /// Intersects per-key row groups across `serving` into `acc`.
    fn intersect(
        &self,
        acc: &mut [Option<RoaringBitmap>],
        serving: &[(&SpecSegments, Serve)],
        keys: &[Vec<u8>],
        spans: &[Vec<std::ops::Range<usize>>],
    ) -> Result<()> {
        for (seg, how) in serving {
            let bitmaps = self.per_key(seg, how, keys, spans)?;
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
        let types = self.column_types(columns)?;
        let (keys, spans) = non_null_probes(keys, &types)?;
        let serving = Self::serving(segs, columns)?;
        let selected = if keys.is_empty() {
            // No key, no row: never a full scan.
            RoaringBitmap::new()
        } else if segs.is_empty() {
            self.locator.live().clone()
        } else {
            let mut acc = vec![None; keys.len()];
            self.intersect(&mut acc, &serving, &keys, &spans)?;
            acc.into_iter()
                .flatten()
                .fold(RoaringBitmap::new(), |u, b| u | b)
        };
        let residual = Predicate::In {
            columns: columns.to_vec(),
            values: expanded(keys.iter()),
        };
        self.finish(&selected, residual)
    }

    fn plan_temporal(
        &self,
        segs: &[SpecSegments],
        columns: &[String],
        keys: &[Vec<u8>],
        asof: Option<i64>,
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
        let mut probes: Vec<Vec<u8>> = variants.iter().flatten().cloned().collect();
        probes.sort_unstable();
        probes.dedup();
        let readers = t
            .sources
            .iter()
            .map(|src| self.open_segment(&t.spec, src, false))
            .collect::<Result<Vec<_>>>()?;
        let hits = self.temporal_hits(&readers, &probes, asof)?;
        // Variants are one key under SQL equality: fold their hits.
        let winners: Vec<Option<VersionHit>> = variants
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
            .collect();
        let mut acc: Vec<Option<RoaringBitmap>> = winners
            .iter()
            .map(|w| Some(w.as_ref().map(|h| h.row_groups.clone()).unwrap_or_default()))
            .collect();
        let others: Vec<(&SpecSegments, Serve)> = serving
            .iter()
            .filter(|(s, _)| !std::ptr::eq(*s, t))
            .map(|(s, h)| (*s, h.clone()))
            .collect();
        self.intersect(&mut acc, &others, keys, &spans)?;
        let selected = acc
            .into_iter()
            .flatten()
            .fold(RoaringBitmap::new(), |u, b| u | b);
        let residual = temporal_residual(
            &t.spec, ts_column, &ts_type, columns, keys, &variants, &winners, asof,
        )?;
        self.finish(&selected, residual)
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
        let serving: Vec<&SpecSegments> = segs
            .iter()
            .filter(|s| match &s.spec {
                KeySpec::Range { column: c } => c == column,
                KeySpec::Exact { columns } | KeySpec::Temporal { columns, .. } => {
                    columns.first().is_some_and(|c| c == column)
                }
                KeySpec::Bloom { .. } => false,
            })
            .collect();
        if !segs.is_empty() && serving.is_empty() {
            return Err(Error::KeySpec(format!(
                "none of the key specs serves a range lookup on {column:?} (a range spec on it, or an \
                 exact or temporal spec led by it)"
            )));
        }
        let mut selected: Option<RoaringBitmap> = None;
        for s in &serving {
            let range = matches!(s.spec, KeySpec::Range { .. });
            let mut found = RoaringBitmap::new();
            for src in &s.sources {
                let r = self.open_segment(&s.spec, src, range)?;
                found |= if range {
                    r.lookup_range(&lo, &hi)?
                } else {
                    r.lookup_prefix_range(&lo, &hi)?
                };
            }
            selected = Some(match selected {
                Some(x) => x & found,
                None => found,
            });
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
        })
    }

    /// Compressed bytes of each row group of tracked file `fi`, by
    /// `index_in_file`: the Parquet footer's row-group sizes, or the
    /// recorded chunk lengths. Both paths verify the file's fingerprint.
    fn row_group_bytes(&self, fi: usize) -> Result<Vec<u64>> {
        let f = &self.manifest().rowgroup_space.files[fi];
        let path = &f.fingerprint.path;
        if let Some(parquet) = &self.parquet {
            let metas = parquet.row_groups(&f.fingerprint)?;
            if metas.len() != f.rowgroup_ids.len() {
                return Err(Error::Source(format!(
                    "{path}: the footer lists {} row groups, the manifest {}",
                    metas.len(),
                    f.rowgroup_ids.len()
                )));
            }
            return Ok(metas.iter().map(|m| m.bytes).collect());
        }
        let chunks = f
            .chunks
            .as_ref()
            .filter(|c| c.len() == f.rowgroup_ids.len())
            .ok_or_else(|| Error::Other(format!("{path}: a text file entry without its chunks")))?;
        let checked = self
            .verified
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&fi);
        if !checked {
            let now = fingerprint_with_policy(&LocalFs, path, self.policy)?;
            if now != f.fingerprint {
                return Err(Error::Source(format!(
                    "{path}: fingerprint drift since manifest sequence {}; run a build",
                    self.sequence()
                )));
            }
            self.verified
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(fi);
        }
        Ok(chunks
            .iter()
            .map(|c| c.end.saturating_sub(c.start))
            .collect())
    }
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

/// Every zero variant of every key, sorted and deduplicated.
fn expanded<'a>(keys: impl Iterator<Item = &'a Vec<u8>>) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = keys.flat_map(|k| zero_variants(k)).collect();
    out.sort_unstable();
    out.dedup();
    out
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
        };
        let exact_ab = KeySpec::Exact {
            columns: cols(&["a", "b"]),
        };
        let bloom_ba = KeySpec::Bloom {
            columns: cols(&["b", "a"]),
            fpr: 0.01,
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
