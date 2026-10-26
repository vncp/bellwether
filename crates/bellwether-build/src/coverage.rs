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
use bellwether_store::{Manifest, RowGroupSpace, SegmentRef, KNOWN_SEGMENT_KINDS};
use bellwether_types::{Error, KeySpec, Result, RowGroupId, SketchSpec};

use crate::target::{declared_sidecars, declared_sketches, declared_targets, Target};
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

/// Whether `seg` is a segment of the key spec or sidecar named `name`: its
/// `kind` is absent for a key spec and the sidecar's kind for a sidecar
/// (whose names always start with `<kind>:`). A segment of a kind this
/// binary does not know is never one (docs/format.md, "Segment kinds").
pub(crate) fn segment_of(seg: &SegmentRef, name: &str) -> bool {
    seg.key_spec == name
        && match seg.kind.as_deref() {
            None => !KNOWN_SEGMENT_KINDS
                .iter()
                .any(|k| name.starts_with(&format!("{k}:"))),
            Some(k) => KNOWN_SEGMENT_KINDS.contains(&k) && name.starts_with(&format!("{k}:")),
        }
}

/// The ids covered by the live segments of the key spec or sidecar named
/// `name`.
pub(crate) fn covered(manifest: &Manifest, name: &str) -> RoaringBitmap {
    let mut out = RoaringBitmap::new();
    for s in manifest.segments.iter().filter(|s| segment_of(s, name)) {
        let (start, end) = (s.rowgroup_range.start.0, s.rowgroup_range.end.0);
        if start < end {
            out.insert_range(start..end);
        }
    }
    out
}

/// Whether the key spec or sidecar named `name` covers every id in `live`.
pub(crate) fn is_complete(manifest: &Manifest, name: &str, live: &RoaringBitmap) -> bool {
    live.is_subset(&covered(manifest, name))
}

/// The sequence at which `spec` became complete in `manifest`, or `None`.
///
/// `Some` only when all of these hold: the manifest declares exactly this
/// spec, it records a completeness sequence under the spec's
/// [`KeySpec::identity`] (storage version 2; every field of the spec, so
/// `bloom:id@0.01` and `bloom:id@0.05` never share an entry), and its live
/// segments cover every live row group. Coverage is recomputed here rather
/// than trusted, so a manifest edited by another writer cannot make a
/// partial spec look complete.
#[must_use]
pub fn completeness(manifest: &Manifest, spec: &KeySpec) -> Option<u64> {
    if !manifest.schema.key_specs.contains(spec) {
        return None;
    }
    recorded_and_covered(manifest, &spec.identity())
}

/// The sequence at which the sidecar `def` became complete in `manifest`,
/// or `None`, by the same rules as [`completeness`]: declared exactly so,
/// recorded, and covering every live row group.
#[must_use]
pub fn sidecar_completeness(manifest: &Manifest, def: &SidecarDef) -> Option<u64> {
    if !declared_sidecars(manifest).contains(def) {
        return None;
    }
    recorded_and_covered(manifest, &def.name())
}

/// The sequence at which the sketch `spec` became complete in `manifest`,
/// or `None`, by the same rules as [`completeness`]. A sketch is read only
/// by `cardinality`, `overlap`, and the `why` report, never by a planner.
#[must_use]
pub fn sketch_completeness(manifest: &Manifest, spec: &SketchSpec) -> Option<u64> {
    if !declared_sketches(manifest).contains(spec) {
        return None;
    }
    recorded_and_covered(manifest, &spec.name())
}

fn recorded_and_covered(manifest: &Manifest, name: &str) -> Option<u64> {
    let seq = *manifest.key_spec_completeness.get(name)?;
    let live = live_ids(&manifest.rowgroup_space).ok()?;
    is_complete(manifest, name, &live).then_some(seq)
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
        if !is_complete(next, &name, &live_next) {
            continue;
        }
        let continued = !replaced.contains(&name)
            && base_targets.contains(target)
            && is_complete(base, &name, &live_base);
        let seq = match base.key_spec_completeness.get(&name) {
            Some(&recorded) if continued => recorded,
            _ => base.sequence + 1,
        };
        out.insert(name, seq);
    }
    // A join key (B3) becomes complete only by its own commit
    // (`Builder::refresh_join`); any later commit keeps its entry while its
    // pair still covers every live left row group, and drops it (the key is
    // then stale on the left) once the left's row groups change.
    for spec in crate::target::declared_joins(&next.schema.key_specs) {
        let name = spec.identity();
        if let Some(&recorded) = base.key_spec_completeness.get(&name) {
            if base.schema.key_specs.contains(&spec)
                && is_complete(base, &name, &live_base)
                && is_complete(next, &name, &live_next)
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
    /// references an id the manifest never assigned is corrupt.
    pub(crate) fn check_known(&self, ids: &RoaringBitmap) -> Result<()> {
        let mut unknown = ids - &self.live;
        unknown -= &self.retired;
        match unknown.min() {
            None => Ok(()),
            Some(id) => Err(Error::Other(format!(
                "a segment references row-group id {id}, which the manifest neither tracks nor retired"
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

    fn seg(name: &str, start: u32, end: u32) -> SegmentRef {
        SegmentRef {
            id: Hash::of(format!("{name}{start}{end}").as_bytes()),
            key_spec: name.into(),
            rowgroup_range: RowGroupRange::new(start, end),
            bytes: 1,
            kind: None,
            right_sequence: None,
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
        assert!(is_complete(&full, "exact:id", &live));
        for gaps in [
            vec![seg("exact:id", 1, 10)],
            vec![seg("exact:id", 0, 4), seg("exact:id", 5, 10)],
            vec![seg("exact:id", 0, 9)],
            vec![],
        ] {
            assert!(
                !is_complete(&manifest(gaps.clone()), "exact:id", &live),
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
        assert!(is_complete(&m, "exact:id", &live));
        // a segment of another spec covering the gap does not help
        let mut other = manifest(vec![seg("exact:id", 0, 5), seg("bloom:id", 5, 10)]);
        other.schema.key_specs.push(KeySpec::Bloom {
            columns: vec!["id".into()],
            fpr: 0.01,
            bounds: None,
        });
        let live = live_ids(&other.rowgroup_space).unwrap();
        assert!(!is_complete(&other, "exact:id", &live));
        assert!(!is_complete(&other, "bloom:id", &live));
    }

    #[test]
    fn an_edge_row_group_with_no_keys_counts_when_its_range_is_recorded() {
        // The writer processed 9 but it held only null keys: the segment's
        // recorded range still covers it.
        let m = manifest(vec![seg("exact:id", 0, 10)]);
        let live = live_ids(&m.rowgroup_space).unwrap();
        assert!(is_complete(&m, "exact:id", &live));
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
        assert!(loc.check_known(&ids).is_err());

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
