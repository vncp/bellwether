//! The plan-building seam ([`PlanSource`]) and the in-crate planner over
//! core segments ([`SegmentPlanner`]).
//!
//! Replay never estimates (ADR 005): a run's cost is the exact sum of the
//! statistics of the row groups a plan selects. The plan comes from a
//! [`PlanSource`]. The build crate's planner implements it
//! ([`crate::build_plan`]); [`SegmentPlanner`] is this crate's
//! implementation over core segments, and follows the build planner's
//! semantics, so both produce the plans the index produces
//! (`tests/wp4_planner.rs`):
//!
//! - **Serving** ([`servable`]). A key spec over columns `P` serves a run
//!   keyed on columns `C` when (a) every column of `P` is in `C`: each key
//!   tuple is projected onto `P`, in `P`'s order, and probed (exact,
//!   temporal, bloom, and range, whose `P` is one column); or (b) `C` is a
//!   strict prefix of `P` and the spec is exact or temporal: each key tuple
//!   is a prefix lookup (ADR 002). Nothing else serves.
//! - **Probes.** A float-zero element expands to both the `+0.0` and the
//!   `-0.0` encoding (`docs/format.md`, statistics semantics).
//! - **Intersection** (ADR 001). With several serving specs, each key
//!   tuple's row groups are the intersection of what every serving spec
//!   returns for it, and the plan is the union over tuples.
//! - **No pruning.** An empty spec list plans every live row group (the
//!   raw-scan configuration). A non-empty list of which no spec serves the
//!   run is an error; the caller decides what an unserved run costs.
//! - **Completeness** (ADR 012). A committed spec plans only if it is
//!   complete at the pinned sequence and its segments' row-group ranges cover
//!   every live row group. A plan that lists any other committed spec is an
//!   error, never a partial plan; a plan that does not list it is
//!   unaffected.
//! - **Ids.** Tombstoned ids in a bitmap are dropped; an id that is neither
//!   live nor tombstoned is an error ([`Catalog::plan_for`]).
//! - **Types.** Keys recorded under another type than the dataset declares
//!   are refused, and so is a transient index whose segments record other
//!   key or timestamp types: a probe at another decimal scale or time unit
//!   would silently miss.
//! - **Residual** (ADR 011). Every plan carries `In` over the run's key
//!   tuples (float zeros in both encodings); an empty key set plans zero row
//!   groups with an empty `In`.

use std::borrow::Cow;
use std::sync::Arc;

use arrow::datatypes::Schema;
use bellwether_core::segment::{ColumnType, ReadAt, SegmentReader};
use bellwether_source::canonical::normalize_float_bounds;
use bellwether_source::partition::{encode_virtual_value, extract_virtual_cols, PathTemplate};
use bellwether_source::DefaultCanonical;
use bellwether_store::{Snapshot, StoreExt};
use bellwether_types::{Error, Hash, KeySpec, Predicate, Result, RowGroupId, RunCost, ScanPlan};
use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::catalog::Catalog;
use crate::keys::RunKeys;
use crate::transient::TransientIndex;

/// How a key spec serves a run's keys ([`servable`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// Probe each key tuple projected onto the spec's columns: indexes into
    /// the run's key columns, in the spec's column order.
    Point {
        /// Run key-column index for each spec column.
        project: Vec<usize>,
    },
    /// The run's columns are a strict prefix of the spec's: each whole key
    /// tuple is a prefix lookup (exact and temporal specs only).
    Prefix,
}

/// How `spec` serves a run keyed on `run_columns`, or `None` if it cannot
/// (module docs, rule (a) then rule (b)).
#[must_use]
pub fn servable(spec: &KeySpec, run_columns: &[String]) -> Option<Lookup> {
    if run_columns.is_empty() {
        return None;
    }
    let p = spec.columns();
    let project: Option<Vec<usize>> = p
        .iter()
        .map(|c| run_columns.iter().position(|r| r == c))
        .collect();
    if let Some(project) = project {
        return Some(Lookup::Point { project });
    }
    let prefix = matches!(spec, KeySpec::Exact { .. } | KeySpec::Temporal { .. })
        && run_columns.len() < p.len()
        && p[..run_columns.len()] == *run_columns;
    prefix.then_some(Lookup::Prefix)
}

/// The surface replay plans through. The build crate's planner implements it
/// ([`crate::build_plan`]); [`SegmentPlanner`] implements it over core
/// segments.
///
/// Every plan is computed against one pinned manifest version (invariant
/// 4) and must contain every row group that holds a queried key (invariant
/// 1). Replay checks each returned plan against the catalog
/// ([`Catalog::verify_plan`]) before costing it, so a misreported plan is an
/// error, never a wrong number.
pub trait PlanSource: Send + Sync {
    /// The manifest sequence (ADR 020) every plan is computed against,
    /// rendered as a string.
    fn manifest_version(&self) -> String;

    /// The key specs this source can plan with.
    fn specs(&self) -> Vec<KeySpec>;

    /// The plan for one run's keys under `specs` (module docs for the
    /// serving, intersection, and no-pruning rules).
    fn plan(&self, specs: &[KeySpec], keys: &RunKeys) -> Result<ScanPlan>;

    /// This source with hypothetical specs' transient segments added, so
    /// each plans exactly as committed segments of that spec would. A
    /// transient index covers every live row group of the pinned catalog,
    /// so it is complete by construction.
    fn with_transient(&self, transient: &[Arc<TransientIndex>])
        -> Result<Box<dyn PlanSource + '_>>;
}

/// The cost of a run under a verified plan: exact sums (ADR 005).
#[must_use]
pub fn run_cost(run_id: &str, keys: u64, plan: &ScanPlan, exact: bool) -> RunCost {
    RunCost {
        run_id: run_id.to_string(),
        keys,
        files: plan.entries.len() as u64,
        row_groups: plan.row_groups_selected,
        bytes: plan.bytes,
        exact,
    }
}

/// Random-access byte ranges of a stored segment.
pub trait RangeRead: Send + Sync {
    /// Total length in bytes.
    fn len(&self) -> u64;
    /// Whether the segment is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Exactly `len` bytes at `offset`.
    fn read_range(&self, offset: u64, len: usize) -> Result<Bytes>;
}

/// The bytes a segment is read from: memory (transient segments) or byte
/// ranges of a committed segment in a pinned snapshot, so a lookup holds one
/// block index plus the blocks it touches (ADR 018).
#[derive(Clone)]
pub enum SegmentBytes {
    /// The whole segment in memory.
    Memory(Bytes),
    /// Read by range on demand.
    Ranged(Arc<dyn RangeRead>),
}

impl std::fmt::Debug for SegmentBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory(b) => write!(f, "Memory({} bytes)", b.len()),
            Self::Ranged(r) => write!(f, "Ranged({} bytes)", r.len()),
        }
    }
}

impl ReadAt for SegmentBytes {
    fn size(&self) -> Result<u64> {
        match self {
            Self::Memory(b) => Ok(b.len() as u64),
            Self::Ranged(r) => Ok(r.len()),
        }
    }

    fn read_at(&self, offset: u64, len: usize) -> Result<Cow<'_, [u8]>> {
        match self {
            Self::Memory(b) => ReadAt::read_at(b, offset, len),
            Self::Ranged(r) => {
                let bytes = r.read_range(offset, len)?;
                if bytes.len() != len {
                    return Err(Error::Store(format!(
                        "segment range read returned {} of {len} bytes",
                        bytes.len()
                    )));
                }
                Ok(Cow::Owned(bytes.to_vec()))
            }
        }
    }
}

/// A committed segment read by range through a pinned [`Snapshot`], which
/// refuses ids its manifest does not reference (invariant 4).
struct SnapshotSegment<S: StoreExt + ?Sized> {
    snapshot: Snapshot<S>,
    id: Hash,
    len: u64,
}

impl<S: StoreExt + ?Sized + Send + Sync> RangeRead for SnapshotSegment<S> {
    fn len(&self) -> u64 {
        self.len
    }

    fn read_range(&self, offset: u64, len: usize) -> Result<Bytes> {
        let end = offset
            .checked_add(len as u64)
            .filter(|&e| e <= self.len)
            .ok_or_else(|| Error::Store(format!("range past the end of segment {}", self.id)))?;
        self.snapshot.read_segment_range(&self.id, offset..end)
    }
}

/// Every segment of one key spec.
#[derive(Debug)]
pub struct SpecSegments {
    spec: KeySpec,
    readers: Vec<SegmentReader<SegmentBytes>>,
}

impl SpecSegments {
    /// The segments of `spec`. Each must index exactly `spec`.
    pub fn new(spec: KeySpec, readers: Vec<SegmentReader<SegmentBytes>>) -> Result<Self> {
        for r in &readers {
            if r.key_spec() != &spec {
                return Err(Error::KeySpec(format!(
                    "a segment indexes {:?}, not {spec:?}",
                    r.key_spec()
                )));
            }
        }
        Ok(Self { spec, readers })
    }

    /// The key spec.
    #[must_use]
    pub fn spec(&self) -> &KeySpec {
        &self.spec
    }

    /// The segment readers.
    #[must_use]
    pub fn readers(&self) -> &[SegmentReader<SegmentBytes>] {
        &self.readers
    }

    /// Row groups that may hold any of `probes` (point probes or prefixes),
    /// unioned across segments.
    fn lookup(&self, probes: &[Vec<u8>], lookup: &Lookup) -> Result<RoaringBitmap> {
        let mut out = RoaringBitmap::new();
        if probes.is_empty() {
            return Ok(out);
        }
        for r in &self.readers {
            match lookup {
                Lookup::Point { .. } => out |= r.lookup(probes)?,
                Lookup::Prefix => {
                    for p in probes {
                        out |= r.lookup_prefix(p)?;
                    }
                }
            }
        }
        Ok(out)
    }
}

/// The in-crate [`PlanSource`] over core segments (module docs).
#[derive(Clone, Debug)]
pub struct SegmentPlanner {
    catalog: Arc<Catalog>,
    specs: Vec<Arc<SpecSegments>>,
    /// Declared specs that are not complete at the pinned sequence, and
    /// why: a plan that lists one is refused.
    incomplete: Vec<(KeySpec, String)>,
    root: Option<String>,
    path_template: Option<PathTemplate>,
}

impl SegmentPlanner {
    /// A planner with no specs: every plan with keys is a raw scan.
    #[must_use]
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            specs: Vec::new(),
            incomplete: Vec::new(),
            root: None,
            path_template: None,
        }
    }

    /// Sets the source root and optional path template for virtual-column
    /// pruning on unindexed catalogs.
    #[must_use]
    pub fn with_source_info(
        mut self,
        root: Option<String>,
        path_template: Option<PathTemplate>,
    ) -> Self {
        self.root = root;
        self.path_template = path_template;
        self
    }

    /// The planner over a pinned snapshot's committed segments: one entry
    /// per declared key spec that is complete at the snapshot's sequence
    /// (ADR 012) with segments whose row-group ranges cover every live row
    /// group. Segments are read by range through the snapshot; each must
    /// index exactly the declared spec and record the dataset schema's key
    /// types, or construction fails. A declared spec that is not complete,
    /// or whose segments leave a gap, is no error here: as in the build
    /// crate's planner, a plan that lists it is refused, and a plan that
    /// does not is unaffected.
    pub fn from_snapshot<S>(snapshot: &Snapshot<S>, catalog: Arc<Catalog>) -> Result<Self>
    where
        S: StoreExt + ?Sized + Send + Sync + 'static,
        Snapshot<S>: Clone,
    {
        let schema = Arc::clone(catalog.schema());
        let manifest = snapshot.manifest();
        let sequence = manifest.sequence;
        if catalog.manifest_version() != sequence.to_string() {
            return Err(Error::Other(format!(
                "catalog is of manifest {} but the snapshot pins {sequence}",
                catalog.manifest_version()
            )));
        }
        let live = catalog.all_ids();
        let root = (!manifest.source.root.is_empty()).then(|| manifest.source.root.clone());
        let path_template = manifest
            .source
            .path_template
            .as_deref()
            .map(PathTemplate::parse)
            .transpose()?;
        let mut planner = Self::new(catalog).with_source_info(root, path_template);
        for (i, spec) in manifest.schema.key_specs.iter().enumerate() {
            let name = spec.name();
            if manifest.schema.key_specs[..i]
                .iter()
                .any(|s| s.name() == name)
            {
                return Err(Error::KeySpec(format!(
                    "two declared key specs share the name {name}; their segments cannot be told \
                     apart"
                )));
            }
            match manifest.key_spec_completeness.get(&name) {
                Some(&at) if at <= sequence => {}
                _ => {
                    planner.incomplete.push((
                        spec.clone(),
                        format!(
                            "key spec {name} is not complete at manifest sequence {sequence} (ADR \
                             012); a plan on an incomplete key spec is an error, never a partial \
                             answer"
                        ),
                    ));
                    continue;
                }
            }
            let refs: Vec<_> = manifest
                .segments
                .iter()
                .filter(|s| s.key_spec == name)
                .collect();
            let mut covered = RoaringBitmap::new();
            for r in &refs {
                if r.rowgroup_range.start.0 < r.rowgroup_range.end.0 {
                    covered.insert_range(r.rowgroup_range.start.0..r.rowgroup_range.end.0);
                }
            }
            let gap = &live - &covered;
            if let Some(first) = gap.min() {
                planner.incomplete.push((
                    spec.clone(),
                    format!(
                        "key spec {name} is marked complete but no segment covers row group \
                         {first} ({} uncovered); refusing a partial plan (ADR 012)",
                        gap.len()
                    ),
                ));
                continue;
            }
            let mut readers = Vec::with_capacity(refs.len());
            for r in refs {
                let bytes = SegmentBytes::Ranged(Arc::new(SnapshotSegment {
                    snapshot: snapshot.clone(),
                    id: r.id,
                    len: r.bytes,
                }));
                readers.push(SegmentReader::open_for_dataset(bytes, &schema)?);
            }
            planner.add(SpecSegments::new(spec.clone(), readers)?)?;
        }
        Ok(planner)
    }

    /// Adds a spec's segments. A spec already present is an error.
    pub fn add(&mut self, segments: SpecSegments) -> Result<()> {
        self.add_shared(Arc::new(segments))
    }

    fn add_shared(&mut self, segments: Arc<SpecSegments>) -> Result<()> {
        if self.find(&segments.spec).is_some() {
            return Err(Error::KeySpec(format!(
                "key spec {:?} already has segments in this planner",
                segments.spec
            )));
        }
        self.specs.push(segments);
        Ok(())
    }

    /// The catalog plans are materialized against.
    #[must_use]
    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    fn find(&self, spec: &KeySpec) -> Option<&Arc<SpecSegments>> {
        self.specs.iter().find(|s| &s.spec == spec)
    }

    /// The segments of a listed spec, or why it cannot be planned with.
    fn listed(&self, spec: &KeySpec) -> Result<&Arc<SpecSegments>> {
        if let Some(s) = self.find(spec) {
            return Ok(s);
        }
        if let Some((_, why)) = self.incomplete.iter().find(|(s, _)| s == spec) {
            return Err(Error::KeySpec(why.clone()));
        }
        Err(Error::KeySpec(format!(
            "no complete segments for {spec:?} at manifest {}",
            self.catalog.manifest_version()
        )))
    }

    /// The selected row-group ids for `keys` under the serving specs.
    fn select(serving: &[(&SpecSegments, Lookup)], keys: &RunKeys) -> Result<RoaringBitmap> {
        let all: Vec<usize> = (0..keys.columns().len()).collect();
        let project = |lookup: &Lookup| match lookup {
            Lookup::Point { project } => project.clone(),
            Lookup::Prefix => all.clone(),
        };
        if let [(segments, lookup)] = serving {
            return segments.lookup(&keys.all_probes(&project(lookup)), lookup);
        }
        // Per key tuple: intersect what every serving spec returns for it,
        // dropping a tuple as soon as its intersection is empty; then union.
        let mut acc: Vec<Option<RoaringBitmap>> = vec![None; keys.len()];
        let mut alive: Vec<usize> = (0..keys.len()).collect();
        for (segments, lookup) in serving {
            let cols = project(lookup);
            let mut next = Vec::with_capacity(alive.len());
            for i in alive {
                let found = segments.lookup(&keys.probes(i, &cols), lookup)?;
                let merged = match acc[i].take() {
                    None => found,
                    Some(prev) => prev & found,
                };
                if !merged.is_empty() {
                    acc[i] = Some(merged);
                    next.push(i);
                }
            }
            alive = next;
        }
        let mut out = RoaringBitmap::new();
        for i in alive {
            if let Some(b) = &acc[i] {
                out |= b;
            }
        }
        Ok(out)
    }

    /// Prunes catalog row groups against virtual columns (Hive `name=value`
    /// directories or `--path-template`), matching the build planner's
    /// `prune_virtual_keys`.
    fn prune_virtual_keys(&self, keys: &RunKeys) -> Result<Option<RoaringBitmap>> {
        let columns = keys.columns();
        let schema = self.catalog.schema();
        let root = self.root.as_deref();
        let mut any_virtual = false;
        let mut file_checks: Vec<Vec<(usize, Vec<u8>, Vec<u8>)>> =
            Vec::with_capacity(self.catalog.files().len());
        for f in self.catalog.files() {
            let vcols = extract_virtual_cols(root, &f.path, self.path_template.as_ref())?;
            let mut checks: Vec<(usize, Vec<u8>, Vec<u8>)> = Vec::new();
            for (ci, col) in columns.iter().enumerate() {
                if let Some(vc) = vcols.iter().find(|v| &v.name == col) {
                    if let Ok(field) = schema.field_with_name(col) {
                        let enc = encode_virtual_value(
                            &DefaultCanonical,
                            &f.path,
                            col,
                            &vc.raw,
                            field.data_type(),
                        )?;
                        let (lo, hi) = normalize_float_bounds(Some(enc.clone()), Some(enc));
                        if let (Some(lo), Some(hi)) = (lo, hi) {
                            checks.push((ci, lo, hi));
                        }
                    }
                }
            }
            if !checks.is_empty() {
                any_virtual = true;
            }
            file_checks.push(checks);
        }
        if !any_virtual {
            return Ok(None);
        }
        let mut matching_files = vec![false; file_checks.len()];
        for (fi, checks) in file_checks.iter().enumerate() {
            if checks.is_empty() {
                matching_files[fi] = true;
                continue;
            }
            matching_files[fi] = (0..keys.len()).any(|ti| {
                checks.iter().all(|(ci, lo, hi)| {
                    let elem = keys.element(ti, *ci);
                    elem >= lo.as_slice() && elem <= hi.as_slice()
                })
            });
        }
        let mut allowed = RoaringBitmap::new();
        for rg in self.catalog.row_groups() {
            if matching_files.get(rg.file).copied().unwrap_or(false) {
                allowed.insert(rg.id.0);
            }
        }
        let _ = RowGroupId(0);
        Ok(Some(allowed))
    }
}

impl PlanSource for SegmentPlanner {
    fn manifest_version(&self) -> String {
        self.catalog.manifest_version().to_string()
    }

    fn specs(&self) -> Vec<KeySpec> {
        self.specs.iter().map(|s| s.spec.clone()).collect()
    }

    fn plan(&self, specs: &[KeySpec], keys: &RunKeys) -> Result<ScanPlan> {
        check_key_types(self.catalog.schema(), keys)?;
        // Every listed spec must be plannable, whether or not it serves
        // these keys, as in the build crate's planner.
        let mut listed = Vec::with_capacity(specs.len());
        for (i, spec) in specs.iter().enumerate() {
            if specs[..i].iter().any(|s| s.name() == spec.name()) {
                return Err(Error::KeySpec(format!(
                    "key spec {} is listed twice",
                    spec.name()
                )));
            }
            listed.push(self.listed(spec)?);
        }
        if keys.is_empty() {
            return Ok(with_residual(
                self.catalog.plan_for(&RoaringBitmap::new())?,
                keys,
            ));
        }
        let vpruned = self.prune_virtual_keys(keys)?;
        if specs.is_empty() {
            let selected = vpruned.unwrap_or_else(|| self.catalog.all_ids());
            return Ok(with_residual(self.catalog.plan_for(&selected)?, keys));
        }
        let mut serving = Vec::with_capacity(specs.len());
        for (spec, segments) in specs.iter().zip(listed) {
            if let Some(lookup) = servable(spec, keys.columns()) {
                serving.push((segments.as_ref(), lookup));
            }
        }
        if serving.is_empty() && vpruned.is_none() {
            return Err(Error::KeySpec(format!(
                "none of {:?} serves keys on {:?}",
                specs.iter().map(KeySpec::name).collect::<Vec<_>>(),
                keys.columns()
            )));
        }
        let mut selected = if serving.is_empty() {
            self.catalog.all_ids()
        } else {
            Self::select(&serving, keys)?
        };
        if let Some(pruned) = vpruned {
            selected &= pruned;
        }
        Ok(with_residual(self.catalog.plan_for(&selected)?, keys))
    }

    fn with_transient(
        &self,
        transient: &[Arc<TransientIndex>],
    ) -> Result<Box<dyn PlanSource + '_>> {
        let mut p = self.clone();
        for t in transient {
            if t.manifest_version() != self.catalog.manifest_version()
                || t.catalog_digest() != self.catalog.digest()
            {
                return Err(Error::Other(format!(
                    "transient index for {} was built over another row-group set (manifest {}, \
                     catalog {})",
                    t.spec().name(),
                    t.manifest_version(),
                    t.catalog_digest()
                )));
            }
            // Keys encoded under another type (decimal scale, time unit)
            // would silently miss: every segment must record exactly the
            // dataset types this planner probes with.
            for r in t.segments().readers() {
                r.check_dataset(self.catalog.schema())?;
            }
            p.add_shared(t.segments())?;
        }
        Ok(Box::new(p))
    }
}

/// Refuses keys recorded under another type than the dataset declares for
/// their columns: probes encoded at another decimal scale or time unit
/// would silently miss.
pub(crate) fn check_key_types(schema: &Schema, keys: &RunKeys) -> Result<()> {
    for (c, recorded) in keys.columns().iter().zip(keys.key_types()) {
        let declared = schema
            .field_with_name(c)
            .map_err(|_| Error::KeySpec(format!("key column {c:?} is not in the dataset")))?
            .data_type();
        if &ColumnType::of(declared)? != recorded {
            return Err(Error::KeySpec(format!(
                "keys on {c:?} were encoded as {recorded:?}, but the dataset declares {declared}"
            )));
        }
    }
    Ok(())
}

/// `plan` with the residual that makes it row-exact (ADR 011): the run's key
/// tuples, float zeros in both encodings. An empty key set gives an empty
/// `In` and no row groups.
fn with_residual(mut plan: ScanPlan, keys: &RunKeys) -> ScanPlan {
    let all: Vec<usize> = (0..keys.columns().len()).collect();
    plan.residual = Some(Predicate::In {
        columns: keys.columns().to_vec(),
        values: keys.all_probes(&all),
    });
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(c: &[&str]) -> Vec<String> {
        c.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn serving_follows_the_planner_rules() {
        let exact = |c: &[&str]| KeySpec::Exact { columns: cols(c) };
        // (a) every spec column in the run's columns: projection, spec order.
        assert_eq!(
            servable(&exact(&["b", "a"]), &cols(&["a", "b"])),
            Some(Lookup::Point {
                project: vec![1, 0]
            })
        );
        assert_eq!(
            servable(&exact(&["a"]), &cols(&["a", "b"])),
            Some(Lookup::Point { project: vec![0] })
        );
        // (b) the run's columns a strict prefix of an exact/temporal spec.
        assert_eq!(
            servable(&exact(&["a", "b"]), &cols(&["a"])),
            Some(Lookup::Prefix)
        );
        let temporal = KeySpec::Temporal {
            columns: cols(&["a", "b"]),
            ts_column: "t".into(),
        };
        assert_eq!(servable(&temporal, &cols(&["a"])), Some(Lookup::Prefix));
        // Neither: a leading column missing, or a bloom spec asked a prefix.
        assert_eq!(servable(&exact(&["b", "a"]), &cols(&["a"])), None);
        assert_eq!(servable(&exact(&["a", "b"]), &cols(&["a", "c"])), None);
        let bloom = KeySpec::Bloom {
            columns: cols(&["a", "b"]),
            fpr: 0.01,
        };
        assert_eq!(servable(&bloom, &cols(&["a"])), None);
        let range = KeySpec::Range { column: "a".into() };
        assert_eq!(
            servable(&range, &cols(&["b", "a"])),
            Some(Lookup::Point { project: vec![1] })
        );
        assert_eq!(servable(&range, &cols(&["b"])), None);
        assert_eq!(servable(&range, &[]), None);
    }
}
