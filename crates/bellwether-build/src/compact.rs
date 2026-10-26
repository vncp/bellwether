//! [`Builder::compact`]: merge each key spec's segments with core's merge.

use std::cell::Cell;
use std::collections::BTreeSet;
use std::sync::Arc;

use bellwether_core::{merge, SegmentReader, WriterOptions};
use bellwether_store::{
    commit_with, segment_path, Extra, Manifest, RowGroupRange, SegmentRef, Snapshot,
    StagedSegments, StoreExt,
};
use bellwether_types::{Error, Hash, KeySpec, Result};
use bytes::Bytes;

use crate::build::Builder;
use crate::config::{columns_to_schema, CompactOptions};
use crate::coverage::{completeness_map, segment_of};
use crate::target::declared_targets;

/// What a compaction did.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompactReport {
    /// The manifest sequence committed, or `None` if nothing was merged.
    pub sequence: Option<u64>,
    /// Merges committed.
    pub merges: u64,
    /// Segments replaced by merges. Their ids are tombstoned by the commit
    /// and deleted by the store's garbage collector after retention.
    pub segments_retired: u64,
    /// The merged segments' manifest entries.
    pub segments_written: Vec<SegmentRef>,
}

/// A planned merge: inputs of one spec whose ranges form one contiguous
/// range, so the output's single recorded range claims nothing its inputs
/// did not cover.
#[derive(Clone, Debug, PartialEq)]
struct Group {
    spec: KeySpec,
    inputs: Vec<SegmentRef>,
}

/// Groups `segs` (one spec, sorted by range start) into merges: consecutive
/// segments whose ranges overlap or touch, up to `max_inputs` segments and
/// `max_output_bytes` of input. Groups of one are dropped.
fn plan_groups(spec: &KeySpec, segs: &[&SegmentRef], opts: &CompactOptions) -> Vec<Group> {
    let mut out = Vec::new();
    let mut current: Vec<SegmentRef> = Vec::new();
    let mut end = 0u32;
    let mut bytes = 0u64;
    let close = |current: &mut Vec<SegmentRef>, out: &mut Vec<Group>| {
        if current.len() >= 2 {
            out.push(Group {
                spec: spec.clone(),
                inputs: std::mem::take(current),
            });
        } else {
            current.clear();
        }
    };
    for s in segs {
        let (start, stop) = (s.rowgroup_range.start.0, s.rowgroup_range.end.0);
        let joins = !current.is_empty()
            && start <= end
            && current.len() < opts.max_inputs
            && bytes.saturating_add(s.bytes) <= opts.max_output_bytes;
        if !joins {
            close(&mut current, &mut out);
            end = 0;
            bytes = 0;
        }
        end = end.max(stop);
        bytes = bytes.saturating_add(s.bytes);
        current.push((*s).clone());
    }
    close(&mut current, &mut out);
    out
}

/// A merge written and uploaded, waiting to be committed.
struct Merged {
    inputs: BTreeSet<Hash>,
    output: SegmentRef,
}

impl<S: StoreExt + ?Sized> Builder<S> {
    /// Merges each declared key spec's segments with core's merge, in
    /// groups whose row-group ranges are contiguous (so the merged
    /// segment's recorded range claims only what its inputs covered) and
    /// whose summed size stays within `max_output_bytes`. Merged segments
    /// are committed in one manifest commit that drops their inputs; the
    /// commit tombstones the inputs, and deleting them is the store's
    /// garbage collector's job (ADR 013).
    ///
    /// Inputs are read whole and checked against their content ids, and
    /// opened against the dataset schema, so segments recorded under
    /// different key types are refused rather than merged. A merge whose
    /// inputs a concurrent commit already dropped is skipped. Coverage is
    /// unchanged, so completeness is kept.
    #[allow(clippy::too_many_lines)]
    pub fn compact(&self, options: &CompactOptions) -> Result<CompactReport> {
        let snapshot = Snapshot::open(Arc::clone(self.store()), self.index_root())?;
        let m = snapshot.manifest();
        let schema = columns_to_schema(&m.schema.columns)?;
        let mut groups = Vec::new();
        for spec in &m.schema.key_specs {
            // A join key (B3) is one segment, rebuilt whole; never merged.
            if matches!(spec, KeySpec::Join { .. }) {
                continue;
            }
            let name = spec.identity();
            let mut segs: Vec<&SegmentRef> = m
                .segments
                .iter()
                .filter(|s| segment_of(s, &name, None))
                .collect();
            segs.sort_by_key(|s| (s.rowgroup_range.start, s.rowgroup_range.end, s.id));
            groups.extend(plan_groups(spec, &segs, options));
        }
        let mut merged = Vec::with_capacity(groups.len());
        for g in &groups {
            let readers = g
                .inputs
                .iter()
                .map(|s| {
                    let r =
                        SegmentReader::open_for_dataset(snapshot.read_segment(&s.id)?, &schema)?;
                    if r.key_spec() != &g.spec {
                        return Err(Error::KeySpec(format!(
                            "segment {} indexes {:?}, but the manifest lists it under {}",
                            s.id,
                            r.key_spec(),
                            g.spec.name()
                        )));
                    }
                    Ok(r)
                })
                .collect::<Result<Vec<SegmentReader<Bytes>>>>()?;
            let out = merge(&readers, &WriterOptions::default())?;
            let bytes = out.bytes.len() as u64;
            self.store().put_content_addressed(
                &segment_path(self.index_root(), &out.id),
                Bytes::from(out.bytes),
            )?;
            let start = g
                .inputs
                .iter()
                .map(|s| s.rowgroup_range.start.0)
                .min()
                .unwrap_or(0);
            let end = g
                .inputs
                .iter()
                .map(|s| s.rowgroup_range.end.0)
                .max()
                .unwrap_or(0);
            merged.push(Merged {
                inputs: g.inputs.iter().map(|s| s.id).collect(),
                output: SegmentRef {
                    id: out.id,
                    key_spec: g.spec.identity(),
                    rowgroup_range: RowGroupRange::new(start, end),
                    bytes,
                    kind: None,
                    join: None,
                    extra: Extra::new(),
                },
            });
        }
        let mut report = CompactReport::default();
        if merged.is_empty() {
            return Ok(report);
        }
        let applied = Cell::new(0u64);
        let retired = Cell::new(0u64);
        let nothing = Cell::new(false);
        let committed = commit_with(
            &**self.store(),
            self.index_root(),
            &StagedSegments::new(),
            options.commit,
            |cur| {
                let (next, n, r) = apply_merges(cur, m, &merged)?;
                if n == 0 {
                    nothing.set(true);
                    return Err(Error::Other("every planned merge was overtaken".into()));
                }
                applied.set(n);
                retired.set(r);
                Ok(next)
            },
        );
        match committed {
            Ok(c) => {
                report.sequence = Some(c.sequence);
                report.merges = applied.get();
                report.segments_retired = retired.get();
                report.segments_written = merged.into_iter().map(|g| g.output).collect();
                Ok(report)
            }
            Err(_) if nothing.get() => Ok(report),
            Err(e) => Err(e),
        }
    }
}

/// Applies every merge whose inputs `cur` still references, provided the
/// schema is the one the merges were read under. Returns the manifest, the
/// merges applied, and the segments retired.
fn apply_merges(
    cur: &Manifest,
    planned: &Manifest,
    merged: &[Merged],
) -> Result<(Manifest, u64, u64)> {
    if cur.schema != planned.schema {
        return Ok((cur.clone(), 0, 0));
    }
    let mut next = cur.clone();
    let (mut applied, mut retired) = (0u64, 0u64);
    for g in merged {
        if !g.inputs.iter().all(|id| next.references(id)) {
            continue;
        }
        next.segments.retain(|s| !g.inputs.contains(&s.id));
        next.add_segment(g.output.clone());
        applied += 1;
        retired += g.inputs.iter().filter(|id| **id != g.output.id).count() as u64;
    }
    // Coverage is unchanged by construction. Recompute completeness from
    // coverage before and after anyway, refuse to publish if they differ,
    // and record the recomputed map (which also drops an entry another
    // writer left for a spec that is not complete).
    let declared = declared_targets(&next);
    let before = completeness_map(cur, cur, &declared, &BTreeSet::new())?;
    let after = completeness_map(cur, &next, &declared, &BTreeSet::new())?;
    if before != after {
        return Err(Error::Other(format!(
            "compaction would change key-spec completeness from {before:?} to {after:?}; nothing was committed"
        )));
    }
    next.key_spec_completeness = after;
    Ok((next, applied, retired))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bellwether_types::Hash;

    fn seg(start: u32, end: u32, bytes: u64) -> SegmentRef {
        SegmentRef {
            id: Hash::of(format!("{start}-{end}").as_bytes()),
            key_spec: "exact:id".into(),
            rowgroup_range: RowGroupRange::new(start, end),
            bytes,
            kind: None,
            join: None,
            extra: Extra::new(),
        }
    }

    fn ranges(groups: &[Group]) -> Vec<Vec<(u32, u32)>> {
        groups
            .iter()
            .map(|g| {
                g.inputs
                    .iter()
                    .map(|s| (s.rowgroup_range.start.0, s.rowgroup_range.end.0))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn groups_never_bridge_a_coverage_gap() {
        let spec = KeySpec::Exact {
            columns: vec!["id".into()],
            counts: false,
            bounds: None,
        };
        let segs = [
            seg(0, 10, 1),
            seg(10, 20, 1),
            seg(30, 40, 1),
            seg(40, 45, 1),
            seg(50, 60, 1),
        ];
        let refs: Vec<&SegmentRef> = segs.iter().collect();
        let groups = plan_groups(&spec, &refs, &CompactOptions::default());
        assert_eq!(
            ranges(&groups),
            vec![vec![(0, 10), (10, 20)], vec![(30, 40), (40, 45)]]
        );
    }

    #[test]
    fn groups_respect_size_and_fan_in_and_accept_overlap() {
        let spec = KeySpec::Exact {
            columns: vec!["id".into()],
            counts: false,
            bounds: None,
        };
        let segs = [
            seg(0, 10, 40),
            seg(5, 12, 40),
            seg(12, 20, 40),
            seg(20, 30, 40),
        ];
        let refs: Vec<&SegmentRef> = segs.iter().collect();
        let small = CompactOptions {
            max_output_bytes: 100,
            ..CompactOptions::default()
        };
        assert_eq!(
            ranges(&plan_groups(&spec, &refs, &small)),
            vec![vec![(0, 10), (5, 12)], vec![(12, 20), (20, 30)]]
        );
        let narrow = CompactOptions {
            max_inputs: 3,
            ..CompactOptions::default()
        };
        assert_eq!(
            ranges(&plan_groups(&spec, &refs, &narrow)),
            vec![vec![(0, 10), (5, 12), (12, 20)]]
        );
    }
}
