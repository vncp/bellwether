//! Row-group ids, segment coverage, and key-spec completeness (ADR 012,
//! ADR 013).
//!
//! - A row-group id is **live** when a tracked file in
//!   `rowgroup_space.files` holds it, and **retired** when a tombstone in
//!   `rowgroup_space.tombstones` lists it. Every id below `next_id` is one
//!   or the other, never both, and ids are never reused.
//! - A segment **covers** the half-open id range its manifest entry records:
//!   the row groups its writer processed, including those that contributed
//!   no keys (a row group whose leading key column is all null). Coverage is
//!   a statement by the builder, not something read back from the segment.
//! - A key spec is **complete** when every live id lies in the union of the
//!   ranges of that spec's live segments. Retired ids need no coverage.
//!
//! `key_spec_completeness[name]` is the sequence of the commit at which the
//! spec became complete. It is kept while the spec stays complete and is
//! dropped by any commit that leaves it incomplete.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bellwether_core::sidecar::SidecarDef;
use bellwether_store::{Manifest, RowGroupSpace, SegmentRef};
use bellwether_types::{Error, KeySpec, Result, RowGroupId, SketchSpec};

use crate::target::{
    declared_sketches, declared_targets, targets_of, timed_sources, Target, TimedSource,
};
use roaring::RoaringBitmap;

/// Every live id, failing if a tracked file lists an id twice or two files
/// share one.
pub(crate) fn live_ids(space: &RowGroupSpace) -> Result<RoaringBitmap> {
    let mut live = RoaringBitmap::new();
    for f in &space.files {
        for id in &f.rowgroup_ids {
            if !live.insert(id.0) {
                return Err(Error::Other(format!(
                    "manifest assigns row-group id {} more than once",
                    id.0
                )));
            }
        }
    }
    Ok(live)
}

/// Every retired id.
pub(crate) fn retired_ids(space: &RowGroupSpace) -> RoaringBitmap {
    space
        .tombstones
        .iter()
        .flat_map(|t| t.rowgroup_ids.iter().map(|id| id.0))
        .collect()
}

/// Whether `seg` is a segment listed under `name` with `kind`: absent for a
/// key spec, the sidecar's kind for a sidecar or a bloom's bounds. A
/// bitemporal key spec and the `temporal2` sidecar of an index written
/// before M5 share a name (`temporal2:id@valid,known`) and differ by kind.
/// A segment of a kind this binary does not know is never one
/// (docs/format.md, "Segment kinds").
pub(crate) fn segment_of(seg: &SegmentRef, name: &str, kind: Option<&str>) -> bool {
    seg.key_spec == name && seg.kind.as_deref() == kind
}

/// The ids covered by the live segments listed under `name` with `kind`.
pub(crate) fn covered(manifest: &Manifest, name: &str, kind: Option<&str>) -> RoaringBitmap {
    let mut out = RoaringBitmap::new();
    for s in manifest
        .segments
        .iter()
        .filter(|s| segment_of(s, name, kind))
    {
        let (start, end) = (s.rowgroup_range.start.0, s.rowgroup_range.end.0);
        if start < end {
            out.insert_range(start..end);
        }
    }
    out
}

/// Whether the segments of `target` cover every id in `live`.
pub(crate) fn is_complete(manifest: &Manifest, target: &Target, live: &RoaringBitmap) -> bool {
    live.is_subset(&covered(manifest, &target.name(), target.kind()))
}

/// Whether every target named `name` among `targets` (a key spec, and a
/// bloom's bounds beside it) covers every id in `live`.
fn all_complete(manifest: &Manifest, targets: &[Target], name: &str, live: &RoaringBitmap) -> bool {
    targets
        .iter()
        .filter(|t| t.name() == name)
        .all(|t| is_complete(manifest, t, live))
}

/// The sequence at which `spec` became complete in `manifest`, or `None`.
///
/// `Some` only when all of these hold: the manifest declares exactly this
/// spec, it records a completeness sequence under the spec's
/// [`KeySpec::identity`] (storage version 2; every field of the spec, so
/// `bloom:id@0.01` and `bloom:id@0.05` never share an entry), and its live
/// segments cover every live row group; for a join key (B3), also every
/// one of its segments pins the manifest's live row-group set
/// (`segments[].join.left_live`), so a left file removed (row groups retired, none
/// added, coverage intact) leaves it incomplete. Coverage is recomputed
/// here rather than trusted, so a manifest edited by another writer cannot
/// make a partial spec look complete.
#[must_use]
pub fn completeness(manifest: &Manifest, spec: &KeySpec) -> Option<u64> {
    if !manifest.schema.key_specs.contains(spec) {
        return None;
    }
    let seq = recorded_and_covered(manifest, &targets_of(spec))?;
    if matches!(spec, KeySpec::Join { .. }) {
        let live = live_ids(&manifest.rowgroup_space).ok()?;
        if !join_pins_live(manifest, &spec.identity(), &live) {
            return None;
        }
    }
    Some(seq)
}

/// Whether the join key named `name` has segments in `manifest` and every
/// one pins `live` as the left's live row-group set it was built over
/// (`segments[].join.left_live`): a pair index describes the left only at
/// the live set it read.
pub(crate) fn join_pins_live(manifest: &Manifest, name: &str, live: &RoaringBitmap) -> bool {
    let id = bellwether_core::join::live_identity(live);
    let mut segs = manifest
        .segments
        .iter()
        .filter(|s| segment_of(s, name, None))
        .peekable();
    segs.peek().is_some() && segs.all(|s| s.join.as_ref().is_some_and(|p| p.left_live == id))
}

/// The sequence at which the per-key bounds or bitemporal versions `def`
/// became complete in `manifest`, or `None`, by the same rules as
/// [`completeness`]: the spec that holds them (an exact or bloom spec with
/// bounds, a bitemporal spec), or the sidecar of an index written before
/// M5, declared exactly so, recorded, and covering every live row group.
#[must_use]
pub fn sidecar_completeness(manifest: &Manifest, def: &SidecarDef) -> Option<u64> {
    let (_, src) = timed_sources(manifest)
        .into_iter()
        .find(|(d, _)| d == def)?;
    match src {
        TimedSource::Spec(spec) | TimedSource::BloomBounds(spec) => completeness(manifest, &spec),
        TimedSource::Legacy => recorded_and_covered(manifest, &[Target::Sidecar(def.clone())]),
    }
}

/// The sequence at which the sketch `spec` became complete in `manifest`,
/// or `None`, by the same rules as [`completeness`]. A sketch is read only
/// by `cardinality`, `overlap`, and the `why` report, never by a planner.
#[must_use]
pub fn sketch_completeness(manifest: &Manifest, spec: &SketchSpec) -> Option<u64> {
    if !declared_sketches(manifest).contains(spec) {
        return None;
    }
    recorded_and_covered(manifest, &[Target::Sketch(spec.clone())])
}

/// The completeness recorded under the name of `targets` (one name: a key
/// spec, and a bloom's bounds beside it), when every one covers every live
/// row group.
fn recorded_and_covered(manifest: &Manifest, targets: &[Target]) -> Option<u64> {
    let name = targets.first()?.name();
    let seq = *manifest.key_spec_completeness.get(&name)?;
    let live = live_ids(&manifest.rowgroup_space).ok()?;
    all_complete(manifest, targets, &name, &live).then_some(seq)
}

/// The completeness map for `next`, a manifest about to be committed on top
/// of `base` (so at sequence `base.sequence + 1`), declaring `declared`
/// (key specs and sidecars, whose names never collide).
///
/// A target is recorded only when `next` covers every live id. One that was
/// already complete in `base`, under the same declaration, and was not
/// rebuilt (`replaced`) keeps its recorded sequence; otherwise it records
/// `base.sequence + 1`. Incomplete targets and undeclared names are absent.
pub(crate) fn completeness_map(
    base: &Manifest,
    next: &Manifest,
    declared: &[Target],
    replaced: &BTreeSet<String>,
) -> Result<BTreeMap<String, u64>> {
    let live_next = live_ids(&next.rowgroup_space)?;
    let live_base = live_ids(&base.rowgroup_space)?;
    let base_targets = declared_targets(base);
    let mut out = BTreeMap::new();
    for target in declared {
        let name = target.name();
        if out.contains_key(&name) || !all_complete(next, declared, &name, &live_next) {
            continue;
        }
        let continued = !replaced.contains(&name)
            && declared
                .iter()
                .filter(|t| t.name() == name)
                .all(|t| base_targets.contains(t))
            && all_complete(base, &base_targets, &name, &live_base);
        let seq = match base.key_spec_completeness.get(&name) {
            Some(&recorded) if continued => recorded,
            _ => base.sequence + 1,
        };
        out.insert(name, seq);
    }
    // A join key (B3) becomes complete only by its own commit
    // (`Builder::refresh_join`); a later commit keeps its entry only while
    // the left's live row groups are exactly the set its pair was built
    // over (a commit that adds a key or compacts changes none), and drops
    // it (the key is then stale on the left) once a file is added,
    // changed, or removed.
    for spec in crate::target::declared_joins(&next.schema.key_specs) {
        let name = spec.identity();
        if let Some(&recorded) = base.key_spec_completeness.get(&name) {
            let target = Target::Key(spec.clone());
            if base.schema.key_specs.contains(&spec)
                && is_complete(base, &target, &live_base)
                && is_complete(next, &target, &live_next)
                && join_pins_live(next, &name, &live_next)
            {
                out.insert(name, recorded);
            }
        }
    }
    Ok(out)
}

/// Maps live row-group ids to `(file index, index_in_file)`, with the file
/// index into `rowgroup_space.files`.
#[derive(Debug, Default)]
pub(crate) struct Locator {
    by_id: HashMap<u32, (usize, u32)>,
    live: RoaringBitmap,
    retired: RoaringBitmap,
}

impl Locator {
    pub(crate) fn new(space: &RowGroupSpace) -> Result<Self> {
        let mut by_id = HashMap::new();
        for (fi, f) in space.files.iter().enumerate() {
            for (idx, id) in f.rowgroup_ids.iter().enumerate() {
                let idx = u32::try_from(idx)
                    .map_err(|_| Error::Other("too many row groups in one file".into()))?;
                if by_id.insert(id.0, (fi, idx)).is_some() {
                    return Err(Error::Other(format!(
                        "manifest assigns row-group id {} more than once",
                        id.0
                    )));
                }
            }
        }
        let live: RoaringBitmap = by_id.keys().copied().collect();
        let retired = retired_ids(space);
        if !live.is_disjoint(&retired) {
            let id = (&live & &retired).min().unwrap_or_default();
            return Err(Error::Other(format!(
                "row-group id {id} is both tracked and retired"
            )));
        }
        if let Some(max) = live.max().max(retired.max()) {
            if max >= space.next_id {
                return Err(Error::Other(format!(
                    "row-group id {max} is at or above next_id {}",
                    space.next_id
                )));
            }
        }
        Ok(Self {
            by_id,
            live,
            retired,
        })
    }

    pub(crate) fn live(&self) -> &RoaringBitmap {
        &self.live
    }

    /// `(file index, index_in_file)` of a live id.
    pub(crate) fn locate(&self, id: RowGroupId) -> Option<(usize, u32)> {
        self.by_id.get(&id.0).copied()
    }

    /// Fails unless every id in `ids` is live or retired: a segment that
    /// references an id the manifest never assigned was built against
    /// another manifest than this one.
    pub(crate) fn check_known(&self, ids: &RoaringBitmap) -> Result<()> {
        let mut unknown = ids - &self.live;
        unknown -= &self.retired;
        match unknown.min() {
            None => Ok(()),
            Some(id) => Err(Error::Other(format!(
                "a segment references row-group id {id}, which this manifest neither tracks nor \
                 retired: the segment was built against another manifest than this one (an \
                 index deleted and rebuilt, or a manifest restored or edited by hand), so its row \
                 groups may name other rows; restore the manifest from manifests/, or delete the \
                 index folder and run `bellwether index` on the data folder with its key specs"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bellwether_store::{Extra, FileEntry, RowGroupRange, RowGroupTombstone, SegmentRef};
    use bellwether_types::{FileFingerprint, Hash};

    fn exact(col: &str) -> KeySpec {
        KeySpec::Exact {
            columns: vec![col.into()],
            counts: false,
            bounds: None,
        }
    }

    fn file(path: &str, ids: &[u32]) -> FileEntry {
        FileEntry {
            fingerprint: FileFingerprint {
                path: path.into(),
                size: 1,
                etag: None,
                tail_blake3: Hash::of(path.as_bytes()),
            },
            rowgroup_ids: ids.iter().map(|&i| RowGroupId(i)).collect(),
            chunk_size: None,
            chunks: None,
            extra: Extra::new(),
            rowgroup_rows: None,
            rowgroup_bytes: None,
            column_bytes: None,
        }
    }

    /// Whether the key spec segments listed under `name` cover `live`.
    fn covers(m: &Manifest, name: &str, live: &RoaringBitmap) -> bool {
        live.is_subset(&covered(m, name, None))
    }

    fn seg(name: &str, start: u32, end: u32) -> SegmentRef {
        SegmentRef {
            id: Hash::of(format!("{name}{start}{end}").as_bytes()),
            key_spec: name.into(),
            rowgroup_range: RowGroupRange::new(start, end),
            bytes: 1,
            kind: None,
            join: None,
            extra: Extra::new(),
        }
    }

    /// Ten live ids 0..10 in two files, spec `exact:id` declared.
    fn manifest(segments: Vec<SegmentRef>) -> Manifest {
        let mut m = Manifest::new();
        m.sequence = 4;
        m.rowgroup_space.next_id = 10;
        m.rowgroup_space.files = vec![file("a", &[0, 1, 2, 3, 4]), file("b", &[5, 6, 7, 8, 9])];
        m.schema.key_specs = vec![exact("id")];
        m.segments = segments;
        m
    }

    #[test]
    fn coverage_gaps_at_start_middle_and_end_are_incomplete() {
        let live = live_ids(&manifest(vec![]).rowgroup_space).unwrap();
        let full = manifest(vec![seg("exact:id", 0, 4), seg("exact:id", 4, 10)]);
        assert!(covers(&full, "exact:id", &live));
        for gaps in [
            vec![seg("exact:id", 1, 10)],
            vec![seg("exact:id", 0, 4), seg("exact:id", 5, 10)],
            vec![seg("exact:id", 0, 9)],
            vec![],
        ] {
            assert!(
                !covers(&manifest(gaps.clone()), "exact:id", &live),
                "{gaps:?}"
            );
        }
    }

    #[test]
    fn retired_ids_need_no_coverage_and_other_specs_do_not_count() {
        // file b rewritten: 5..10 retired, fresh ids 10..15
        let mut m = manifest(vec![seg("exact:id", 0, 5), seg("exact:id", 10, 15)]);
        m.rowgroup_space.files[1] = file("b", &[10, 11, 12, 13, 14]);
        m.rowgroup_space.next_id = 15;
        m.rowgroup_space.tombstones.push(RowGroupTombstone {
            since: "2026-10-24T00:00:00.000Z".into(),
            rowgroup_ids: (5..10).map(RowGroupId).collect(),
            extra: Extra::new(),
        });
        let live = live_ids(&m.rowgroup_space).unwrap();
        assert!(covers(&m, "exact:id", &live));
        // a segment of another spec covering the gap does not help
        let mut other = manifest(vec![seg("exact:id", 0, 5), seg("bloom:id", 5, 10)]);
        other.schema.key_specs.push(KeySpec::Bloom {
            columns: vec!["id".into()],
            fpr: 0.01,
            bounds: None,
        });
        let live = live_ids(&other.rowgroup_space).unwrap();
        assert!(!covers(&other, "exact:id", &live));
        assert!(!covers(&other, "bloom:id", &live));
    }

    #[test]
    fn an_edge_row_group_with_no_keys_counts_when_its_range_is_recorded() {
        // The writer processed 9 but it held only null keys: the segment's
        // recorded range still covers it.
        let m = manifest(vec![seg("exact:id", 0, 10)]);
        let live = live_ids(&m.rowgroup_space).unwrap();
        assert!(covers(&m, "exact:id", &live));
    }

    #[test]
    fn completeness_is_recorded_only_when_total() {
        let spec = exact("id");
        let declared = vec![Target::Key(spec.clone())];
        let base = manifest(vec![seg("exact:id", 0, 5)]);
        // next covers only 0..5: not recorded
        let partial = completeness_map(&base, &base, &declared, &BTreeSet::new()).unwrap();
        assert!(partial.is_empty());
        // next covers everything: recorded at base.sequence + 1
        let next = manifest(vec![seg("exact:id", 0, 5), seg("exact:id", 5, 10)]);
        let map = completeness_map(&base, &next, &declared, &BTreeSet::new()).unwrap();
        assert_eq!(map.get("exact:id"), Some(&5));
        // continuously complete: the original sequence is kept
        let mut complete_base = next.clone();
        complete_base
            .key_spec_completeness
            .insert("exact:id".into(), 2);
        let mut grown = next.clone();
        grown.rowgroup_space.files.push(file("c", &[10]));
        grown.rowgroup_space.next_id = 11;
        grown.segments.push(seg("exact:id", 10, 11));
        let map = completeness_map(&complete_base, &grown, &declared, &BTreeSet::new()).unwrap();
        assert_eq!(map.get("exact:id"), Some(&2));
        // a rebuilt spec records the new sequence even if it was complete
        let replaced: BTreeSet<String> = ["exact:id".to_string()].into();
        let map = completeness_map(&complete_base, &grown, &declared, &replaced).unwrap();
        assert_eq!(map.get("exact:id"), Some(&5));
        // a new file without coverage drops the entry
        let mut uncovered = next;
        uncovered.rowgroup_space.files.push(file("c", &[10]));
        uncovered.rowgroup_space.next_id = 11;
        let map =
            completeness_map(&complete_base, &uncovered, &declared, &BTreeSet::new()).unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn public_completeness_rechecks_declaration_and_coverage() {
        let spec = exact("id");
        let mut m = manifest(vec![seg("exact:id", 0, 10)]);
        assert_eq!(completeness(&m, &spec), None, "nothing recorded");
        m.key_spec_completeness.insert("exact:id".into(), 3);
        assert_eq!(completeness(&m, &spec), Some(3));
        // same name, different spec
        let temporal = KeySpec::Temporal {
            columns: vec!["id".into()],
            ts_column: "ts".into(),
            counts: false,
        };
        m.schema.key_specs = vec![temporal.clone()];
        m.key_spec_completeness.insert("temporal:id@ts".into(), 3);
        m.segments[0].key_spec = "temporal:id@ts".into();
        let other_ts = KeySpec::Temporal {
            columns: vec!["id".into()],
            ts_column: "updated_at".into(),
            counts: false,
        };
        assert_eq!(temporal.name(), other_ts.name());
        assert_eq!(completeness(&m, &temporal), Some(3));
        assert_eq!(completeness(&m, &other_ts), None);
        // recorded but a segment went missing
        m.segments.clear();
        assert_eq!(completeness(&m, &temporal), None);
    }

    /// Completeness and segments are keyed by the full identity (storage
    /// version 2): a bloom spec at another false-positive rate, which
    /// shares its name, never inherits the old spec's entry or segments.
    #[test]
    fn completeness_is_keyed_by_identity_not_name() {
        let at = |fpr: f64| KeySpec::Bloom {
            columns: vec!["id".into()],
            fpr,
            bounds: None,
        };
        let (old, new) = (at(0.01), at(0.05));
        assert_eq!(old.name(), new.name());
        let mut m = manifest(vec![seg("bloom:id@0.01", 0, 10)]);
        m.schema.key_specs = vec![old.clone()];
        m.key_spec_completeness.insert("bloom:id@0.01".into(), 3);
        assert_eq!(completeness(&m, &old), Some(3));
        // the redefinition is declared, the old entry and segments remain
        m.schema.key_specs = vec![new.clone()];
        assert_eq!(completeness(&m, &new), None);
        let declared = vec![Target::Key(new)];
        let map = completeness_map(&m, &m, &declared, &BTreeSet::new()).unwrap();
        assert!(map.is_empty(), "{map:?}");
    }

    #[test]
    fn locator_refuses_inconsistent_spaces() {
        let m = manifest(vec![]);
        let loc = Locator::new(&m.rowgroup_space).unwrap();
        assert_eq!(loc.locate(RowGroupId(7)), Some((1, 2)));
        assert_eq!(loc.locate(RowGroupId(10)), None);
        let mut ids = RoaringBitmap::new();
        ids.insert(3);
        assert!(loc.check_known(&ids).is_ok());
        ids.insert(10);
        // Review of PR #32, item 7c: the true cause and the next step.
        let msg = loc.check_known(&ids).unwrap_err().to_string();
        assert!(msg.contains("row-group id 10"), "{msg}");
        assert!(msg.contains("built against another manifest"), "{msg}");
        assert!(
            msg.contains("restore the manifest from manifests/"),
            "{msg}"
        );
        assert!(
            msg.contains("`bellwether index` on the data folder"),
            "{msg}"
        );

        let mut dup = m.clone();
        dup.rowgroup_space.files[1].rowgroup_ids[0] = RowGroupId(0);
        assert!(Locator::new(&dup.rowgroup_space).is_err());
        assert!(live_ids(&dup.rowgroup_space).is_err());

        let mut both = m.clone();
        both.rowgroup_space.tombstones.push(RowGroupTombstone {
            since: "2026-10-24T00:00:00.000Z".into(),
            rowgroup_ids: vec![RowGroupId(3)],
            extra: Extra::new(),
        });
        assert!(Locator::new(&both.rowgroup_space).is_err());

        let mut high = m;
        high.rowgroup_space.next_id = 9;
        assert!(Locator::new(&high.rowgroup_space).is_err());
    }
}
