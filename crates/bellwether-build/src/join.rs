//! Join keys (B3): building a dataset pair's pair index into the left
//! index ([`Builder::refresh_join`]), and planning semi-joins and overlap
//! statistics with it ([`Planner::plan_semijoin`],
//! [`Planner::join_overlap`]).
//!
//! A join key is declared on the left dataset like any key spec, but it is
//! not built row group by row group: [`Builder::refresh_join`] builds it
//! whole from both datasets' complete exact keys on the join columns (core's
//! `join::build_pair`), split by key range into segments capped like any
//! other (ADR 018), in one commit to the left index that pins what it was
//! built from in every segment's header and manifest entry (ADR 020): the
//! right manifest by sequence and content hash, and the left's live
//! row-group set. It is recorded complete. A later left commit keeps it
//! complete only while the left's live row groups are exactly that set;
//! once a left file is added, changed, or removed it is incomplete (stale
//! on the left), and once the right dataset's current manifest is another
//! (a later commit, or the same sequence of an index deleted and rebuilt)
//! it is stale on the right ([`JoinState`]). A stale key is never planned
//! with: a semi-join planned from it could miss rows on the side that
//! moved (invariant 1 per side).

use std::cell::Cell;
use std::sync::Arc;

use bellwether_core::join::{self as cj, OverlapStats, PairCaps, Pins, Side};
use bellwether_core::segment::{ColumnType, EncodedSegment, SegmentReader, WriterOptions};
use bellwether_store::{
    commit_with, segment_path, CommitOptions, Extra, JoinPin, Manifest, RowGroupRange, SegmentRef,
    Snapshot, StagedSegments, StoreExt,
};
use bellwether_types::{Error, Hash, KeySpec, Predicate, Quantity, Result, ScanPlan};
use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::build::Builder;
use crate::config::{columns_to_schema, DEFAULT_MAX_SEGMENT_BYTES, DEFAULT_MAX_SEGMENT_KEYS};
use crate::coverage::{completeness, live_ids, segment_of};
use crate::plan::Planner;
use crate::seg_io::SegmentBytes;

/// How many times [`Builder::refresh_join`] starts over after a concurrent
/// commit changed the left's row groups or schema under it.
const MAX_JOIN_RESTARTS: u32 = 8;

/// The pair segment caps a build uses by default: the build's own segment
/// caps ([`DEFAULT_MAX_SEGMENT_KEYS`] shared keys, [`DEFAULT_MAX_SEGMENT_BYTES`]
/// bytes).
pub const DEFAULT_PAIR_CAPS: PairCaps = PairCaps {
    max_keys: DEFAULT_MAX_SEGMENT_KEYS,
    max_bytes: DEFAULT_MAX_SEGMENT_BYTES,
};

/// A dataset's current manifest as a join key pins its right side: by
/// sequence and by content hash ([`Snapshot::content_hash`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RightManifest {
    /// The manifest sequence.
    pub sequence: u64,
    /// BLAKE3 of the manifest bytes as stored.
    pub hash: Hash,
}

impl RightManifest {
    /// The manifest `snapshot` pins.
    #[must_use]
    pub fn of<S: StoreExt + ?Sized>(snapshot: &Snapshot<S>) -> Self {
        Self {
            sequence: snapshot.sequence(),
            hash: snapshot.content_hash(),
        }
    }
}

/// Whether a declared join key can be planned with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinState {
    /// Built over the left's live row groups as they are, and pinned to the
    /// right dataset's current manifest.
    Fresh {
        /// The left sequence at which the key became complete.
        complete_at: u64,
        /// The right sequence it is pinned to (the current one).
        right_sequence: u64,
    },
    /// Declared, and no pair index built yet.
    NotBuilt,
    /// The left's live row groups changed since the pair index was built
    /// (a file added, changed, or removed).
    LeftChanged {
        /// The left sequence whose exact keys the pair was built from.
        built_from: u64,
        /// The left's current sequence.
        current: u64,
    },
    /// The right dataset committed again since the pair index was built.
    RightAdvanced {
        /// The right sequence the pair is pinned to.
        pinned: u64,
        /// The right's current sequence.
        current: u64,
    },
    /// The right dataset's current manifest is not the one the pair was
    /// built at, though its sequence is not later: the right index was
    /// deleted and rebuilt (or its manifest replaced), so its row-group ids
    /// may name other rows.
    RightRebuilt {
        /// The right sequence the pair is pinned to.
        pinned: u64,
        /// The right's current sequence (another manifest).
        current: u64,
    },
}

impl JoinState {
    /// The state in words, for a refusal.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::Fresh { right_sequence, .. } => {
                format!("fresh, pinned to the right dataset's sequence {right_sequence}")
            }
            Self::NotBuilt => "declared, and its pair index is not built yet".into(),
            Self::LeftChanged {
                built_from,
                current,
            } => format!(
                "stale on the left: built from the left's sequence {built_from}, and the left's \
                 row groups changed by sequence {current} (a file added, changed, or removed)"
            ),
            Self::RightAdvanced { pinned, current } => format!(
                "stale on the right: pinned to the right dataset's sequence {pinned}, which is \
                 now at {current}"
            ),
            Self::RightRebuilt { pinned, current } => format!(
                "stale on the right: pinned to the right dataset's manifest at sequence {pinned}, \
                 and the right index now holds another manifest at sequence {current}: it was \
                 deleted and rebuilt (or its manifest replaced), so its row-group ids may name \
                 other rows"
            ),
        }
    }
}

/// What [`Builder::refresh_join`] did.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum JoinRefresh {
    /// The pair index was already fresh; nothing was committed.
    Fresh {
        /// The right sequence it is pinned to.
        right_sequence: u64,
    },
    /// The pair index was built and committed.
    Built {
        /// The left sequence committed.
        sequence: u64,
        /// The right sequence it is pinned to.
        right_sequence: u64,
        /// The exact key the left side was read from.
        left_exact: KeySpec,
        /// The exact key the right side was read from.
        right_exact: KeySpec,
        /// The pair segments' manifest entries, in key order.
        segments: Vec<SegmentRef>,
    },
    /// The right dataset's current manifest is not the one the caller
    /// expected (it committed, or was rebuilt, since the caller read it);
    /// nothing was built or committed.
    RightChanged {
        /// The right manifest's content hash the caller expected.
        expected: Hash,
        /// The right manifest found.
        found: RightManifest,
    },
}

/// The two-step plan of `semijoin(keys)` ([`Planner::plan_semijoin`]).
#[derive(Clone, Debug, PartialEq)]
pub struct SemiJoinPlan {
    /// Distinct non-null keys asked.
    pub keys: u64,
    /// Those both datasets hold (the rest are on one side or neither, and
    /// drop out of a semi-join).
    pub matched: u64,
    /// Step 1, the left dataset: the row groups holding a matched key (what
    /// `left ⋉ right` reads for these keys), residual `In` over the matched
    /// keys on the left columns. With counts, `rows` is exact.
    pub left: ScanPlan,
    /// Step 2, the right dataset: the row groups holding a matched key,
    /// residual `In` over the matched keys on the right columns.
    pub right: ScanPlan,
    /// The right manifest sequence the plan's right side is of.
    pub right_sequence: u64,
}

/// The complete exact key in `m` a join side is read from: exact on exactly
/// `columns` with no time bounds, counting when the join key counts. `None`
/// when the index has none.
#[must_use]
pub fn join_side_spec(m: &Manifest, columns: &[String], counts: bool) -> Option<KeySpec> {
    let fits = |s: &&KeySpec| {
        matches!(s, KeySpec::Exact { columns: c, bounds: None, counts: n }
            if c.as_slice() == columns && (*n || !counts))
    };
    let complete: Vec<&KeySpec> = m
        .schema
        .key_specs
        .iter()
        .filter(fits)
        .filter(|s| completeness(m, s).is_some())
        .collect();
    // An uncounted join key prefers the uncounted exact key (smaller values).
    complete
        .iter()
        .find(|s| !s.counts() || counts)
        .or_else(|| complete.first())
        .map(|s| (*s).clone())
}

/// The join key's pair segments listed in `m`.
fn pair_segments<'m>(m: &'m Manifest, spec: &KeySpec) -> Vec<&'m SegmentRef> {
    let identity = spec.identity();
    m.segments
        .iter()
        .filter(|s| segment_of(s, &identity))
        .collect()
}

/// The dataset types of `columns` in `m`'s schema, as segments record them.
fn column_types(m: &Manifest, columns: &[String]) -> Result<Vec<ColumnType>> {
    let schema = columns_to_schema(&m.schema.columns)?;
    columns
        .iter()
        .map(|c| {
            let f = schema
                .field_with_name(c)
                .map_err(|_| Error::KeySpec(format!("join column {c} is not in the dataset")))?;
            ColumnType::of(f.data_type())
        })
        .collect()
}

/// Column types in words: `Int64, Utf8`.
fn types_in_words(types: &[ColumnType]) -> String {
    types
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Opens every segment of the complete `spec` in `snapshot`, by range (one
/// block at a time as the merge walks them), against its dataset schema.
fn open_side<'a, S: StoreExt + ?Sized>(
    snapshot: &'a Snapshot<S>,
    spec: &KeySpec,
) -> Result<Vec<SegmentReader<SegmentBytes<'a, S>>>> {
    let m = snapshot.manifest();
    let schema = columns_to_schema(&m.schema.columns)?;
    let identity = spec.identity();
    m.segments
        .iter()
        .filter(|s| segment_of(s, &identity))
        .map(|s| {
            let r = SegmentReader::open_for_dataset(
                SegmentBytes::Ranged {
                    snapshot,
                    id: s.id,
                    size: s.bytes,
                },
                &schema,
            )?;
            if r.key_spec() != spec {
                return Err(Error::KeySpec(format!(
                    "a segment listed under {identity} indexes {}; restore the manifest from \
                     manifests/ or rebuild the spec",
                    r.key_spec().identity()
                )));
            }
            Ok(r)
        })
        .collect()
}

/// The refusal for a side with no complete exact key on its join columns.
fn missing_side(
    spec: &KeySpec,
    side: &str,
    dataset: &str,
    columns: &[String],
    counts: bool,
) -> Error {
    let need = cj::side_spec(columns, counts);
    Error::NotIndexed {
        root: dataset.to_string(),
        operation: "a join key",
        needs: Some(format!(
            "{} (the {side} side of {}, which is built from both datasets' complete exact keys; \
             a missing one is an error, never a guess)",
            need.identity(),
            spec.name()
        )),
    }
}

/// A join key's pair index built from both datasets' complete exact keys,
/// not committed anywhere: the transient pair index `why` prices a join key
/// with before it exists ([`transient_pair`]), the same bytes
/// [`Builder::refresh_join`] commits.
#[derive(Clone, Debug)]
pub struct PairBuild {
    /// The pair segments' bytes (`keyed_join` layout), in key order.
    pub segments: Vec<Vec<u8>>,
    /// The exact key the left side was read from.
    pub left_exact: KeySpec,
    /// The exact key the right side was read from.
    pub right_exact: KeySpec,
    /// The left manifest sequence it was built from.
    pub left_sequence: u64,
    /// What it pins (every segment's manifest entry records it).
    pub pin: JoinPin,
    /// The left row groups each segment covers, as its manifest entry
    /// records them.
    pub rowgroup_range: RowGroupRange,
}

/// What [`build_segments`] built, besides the segments it emitted.
struct PairMeta {
    left_exact: KeySpec,
    right_exact: KeySpec,
    left_sequence: u64,
    pin: JoinPin,
    rowgroup_range: RowGroupRange,
}

/// Builds the pair segments of the join key `spec` from `left` (the left
/// dataset's index, where `spec` is declared or about to be) and `right`
/// (the right dataset's), handing each to `emit` as it is finished: from
/// the complete exact keys on the join columns on each side, with counts
/// when `spec` counts. A side with no such key is refused with
/// [`Error::NotIndexed`] naming the spec to declare there: a missing exact
/// key is an error, never a guess. Reads the two exact keys one block per
/// segment at a time and holds one pair segment being written, capped by
/// `caps` (ADR 018).
fn build_segments<S: StoreExt + ?Sized, R: StoreExt + ?Sized>(
    left: &Snapshot<S>,
    right: &Snapshot<R>,
    spec: &KeySpec,
    caps: PairCaps,
    emit: impl FnMut(EncodedSegment) -> Result<()>,
) -> Result<PairMeta> {
    let KeySpec::Join {
        dataset,
        columns,
        right: right_name,
        right_columns,
        counts,
    } = spec
    else {
        return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
    };
    cj::check_spec(spec)?;
    let (m, r) = (left.manifest(), right.manifest());
    let left_exact = join_side_spec(m, columns, *counts)
        .ok_or_else(|| missing_side(spec, "left", dataset, columns, *counts))?;
    let right_exact = join_side_spec(r, right_columns, *counts)
        .ok_or_else(|| missing_side(spec, "right", right_name, right_columns, *counts))?;
    let left_types = column_types(m, columns)?;
    let right_types = column_types(r, right_columns)?;
    cj::check_comparable(spec, &left_types, &right_types)?;
    let lreaders = open_side(left, &left_exact)?;
    let rreaders = open_side(right, &right_exact)?;
    let llive = live_ids(&m.rowgroup_space)?;
    let rlive = live_ids(&r.rowgroup_space)?;
    let range = llive.min().zip(llive.max());
    let now = RightManifest::of(right);
    cj::build_pair(
        spec,
        &Side {
            segments: &lreaders,
            live: &llive,
            types: Some(left_types),
        },
        &Side {
            segments: &rreaders,
            live: &rlive,
            types: Some(right_types),
        },
        range,
        Pins {
            left: m.sequence,
            right: now.sequence,
            right_manifest: now.hash,
        },
        &WriterOptions::default(),
        caps,
        emit,
    )?;
    Ok(PairMeta {
        left_exact,
        right_exact,
        left_sequence: m.sequence,
        pin: JoinPin {
            right_sequence: now.sequence,
            right_manifest: now.hash,
            left_live: cj::live_identity(&llive),
            extra: Extra::new(),
        },
        rowgroup_range: range.map_or(RowGroupRange::new(0, 0), |(a, b)| {
            RowGroupRange::new(a, b.saturating_add(1))
        }),
    })
}

/// Builds the pair index of the join key `spec` from `left` (the left
/// dataset's index) and `right` (the right dataset's) without committing
/// it: what `why` prices a join key with, the same segments
/// [`Builder::refresh_join`] commits, so a price computed from them is the
/// plan. The segments are split at `caps` and held in memory, all of them
/// at most `max_bytes` (a transient index's cap, ADR 018); a pair past it
/// is refused, never truncated.
pub fn transient_pair<S: StoreExt + ?Sized, R: StoreExt + ?Sized>(
    left: &Snapshot<S>,
    right: &Snapshot<R>,
    spec: &KeySpec,
    caps: PairCaps,
    max_bytes: u64,
) -> Result<PairBuild> {
    let mut segments: Vec<Vec<u8>> = Vec::new();
    let mut held = 0u64;
    let meta = build_segments(left, right, spec, caps, |seg| {
        held = held.saturating_add(seg.bytes.len() as u64);
        if held > max_bytes {
            return Err(Error::Other(format!(
                "the transient pair index of {} would hold more than {max_bytes} bytes, past \
                 its cap on a transient index (TransientLimits::max_index_bytes), so it is not \
                 priced in memory; declare it with `bellwether index <left folder> --key {}` to \
                 build it into the index, segment by segment, and measure it there",
                spec.name(),
                spec.identity()
            )));
        }
        segments.push(seg.bytes);
        Ok(())
    })?;
    Ok(PairBuild {
        segments,
        left_exact: meta.left_exact,
        right_exact: meta.right_exact,
        left_sequence: meta.left_sequence,
        pin: meta.pin,
        rowgroup_range: meta.rowgroup_range,
    })
}

/// The declarations after declaring the join key `spec` in `m`: `spec`
/// added, or replacing the spec of its name (another `counts`), checked
/// against the left dataset as a build checks a declared join key; and
/// the identity it replaces, if any.
fn declare_join(m: &Manifest, spec: &KeySpec) -> Result<(Vec<KeySpec>, Option<String>)> {
    if m.schema.key_specs.contains(spec) {
        return Ok((m.schema.key_specs.clone(), None));
    }
    let replaced = m
        .schema
        .key_specs
        .iter()
        .find(|s| s.name() == spec.name())
        .map(KeySpec::identity);
    let declared = crate::build::declared_specs(Some(m), std::slice::from_ref(spec), false)?;
    let schema = columns_to_schema(&m.schema.columns)?;
    crate::build::check_joins(&m.source.root, &declared, &schema)?;
    if declared.len() + m.schema.sidecars.len() > crate::MAX_KEY_SPECS {
        return Err(Error::KeySpec(format!(
            "declaring {} would make {} key specs and {} sidecars; at most {} are allowed per \
             index",
            spec.name(),
            declared.len(),
            m.schema.sidecars.len(),
            crate::MAX_KEY_SPECS
        )));
    }
    Ok((declared, replaced))
}

impl<S: StoreExt + ?Sized> Builder<S> {
    /// Builds the join key `spec` (B3) into this (the left) index, if it is
    /// not fresh, declaring it in the same commit when the index does not
    /// declare it yet: from the complete exact keys on its columns in this
    /// index and in the right dataset's index (`right_store`, `right_root`),
    /// split into segments at `caps`, each uploaded as it is finished, then
    /// one commit that replaces its pair segments, pins the right manifest
    /// and the left's live row groups, and records it complete. With
    /// `expect`, the right manifest's content hash the caller verified
    /// against: a right dataset whose current manifest is another is
    /// [`JoinRefresh::RightChanged`], before anything is built or
    /// committed. Starts over if a concurrent commit changes the left's row
    /// groups or schema before it lands.
    pub fn refresh_join<R: StoreExt + ?Sized>(
        &self,
        spec: &KeySpec,
        right_store: &Arc<R>,
        right_root: &str,
        expect: Option<Hash>,
        caps: PairCaps,
    ) -> Result<JoinRefresh> {
        let mut restarts = 0u32;
        loop {
            if let Some(done) = self.try_refresh_join(spec, right_store, right_root, expect, caps)? {
                return Ok(done);
            }
            restarts += 1;
            if restarts > MAX_JOIN_RESTARTS {
                return Err(Error::VersionConflict {
                    expected: "a left manifest stable for the length of one join build".into(),
                    found: format!("{restarts} conflicting commits"),
                });
            }
        }
    }

    fn try_refresh_join<R: StoreExt + ?Sized>(
        &self,
        spec: &KeySpec,
        right_store: &Arc<R>,
        right_root: &str,
        expect: Option<Hash>,
        caps: PairCaps,
    ) -> Result<Option<JoinRefresh>> {
        if !matches!(spec, KeySpec::Join { .. }) {
            return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
        }
        let left = Snapshot::open(Arc::clone(self.store()), self.index_root())?;
        let m = left.manifest();
        let rsnap = Snapshot::open(Arc::clone(right_store), right_root)?;
        let now = RightManifest::of(&rsnap);
        if let Some(expected) = expect.filter(|e| *e != now.hash) {
            return Ok(Some(JoinRefresh::RightChanged {
                expected,
                found: now,
            }));
        }
        if m.schema.key_specs.contains(spec) {
            let state = state_of(m, spec, now, || Ok(Some(pair_built_from(&left, spec)?)))?;
            if let JoinState::Fresh { right_sequence, .. } = state {
                return Ok(Some(JoinRefresh::Fresh { right_sequence }));
            }
        }
        let (declared, replaced) = declare_join(m, spec)?;
        let mut written: Vec<(Hash, u64)> = Vec::new();
        let meta = build_segments(&left, &rsnap, spec, caps, |seg| {
            let bytes = seg.bytes.len() as u64;
            self.store().put_content_addressed(
                &segment_path(self.index_root(), &seg.id),
                Bytes::from(seg.bytes),
            )?;
            written.push((seg.id, bytes));
            Ok(())
        })?;
        let identity = spec.identity();
        let segments: Vec<SegmentRef> = written
            .into_iter()
            .map(|(id, bytes)| SegmentRef {
                id,
                kind: None,
                key_spec: identity.clone(),
                rowgroup_range: meta.rowgroup_range,
                bytes,
                join: Some(meta.pin.clone()),
                extra: Extra::new(),
            })
            .collect();
        let (space, schema) = (m.rowgroup_space.clone(), m.schema.clone());
        let restart = Cell::new(false);
        let committed = commit_with(
            &**self.store(),
            self.index_root(),
            &StagedSegments::new(),
            CommitOptions::default(),
            |cur| {
                // The pair's left bitmaps hold the ids of the row groups it
                // read; another assignment, or another declaration, cannot
                // take it.
                if cur.rowgroup_space != space || cur.schema != schema {
                    restart.set(true);
                    return Err(Error::VersionConflict {
                        expected: "the left manifest the join key was built from".into(),
                        found: format!("sequence {}", cur.sequence),
                    });
                }
                let mut next = cur.clone();
                next.schema.key_specs.clone_from(&declared);
                next.segments.retain(|s| {
                    !(s.kind.is_none()
                        && (s.key_spec == identity || replaced.as_ref() == Some(&s.key_spec)))
                });
                if let Some(old) = &replaced {
                    next.key_spec_completeness.remove(old);
                }
                for s in &segments {
                    next.add_segment(s.clone());
                }
                next.key_spec_completeness
                    .insert(identity.clone(), cur.sequence + 1);
                Ok(next)
            },
        );
        match committed {
            Ok(c) => Ok(Some(JoinRefresh::Built {
                sequence: c.sequence,
                right_sequence: meta.pin.right_sequence,
                left_exact: meta.left_exact,
                right_exact: meta.right_exact,
                segments,
            })),
            Err(_) if restart.get() => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// The left sequence a join key's pair segment was built from, read from
/// its header.
fn pair_built_from<S: StoreExt + ?Sized>(snapshot: &Snapshot<S>, spec: &KeySpec) -> Result<u64> {
    let m = snapshot.manifest();
    let Some(s) = pair_segments(m, spec).first().copied() else {
        return Ok(0);
    };
    let r = SegmentReader::open(SegmentBytes::Ranged {
        snapshot,
        id: s.id,
        size: s.bytes,
    })?;
    Ok(cj::header_pins(&r)?.left_sequence)
}

/// The state of `spec` in `m` against a right dataset whose current
/// manifest is `right_now`. `built_from` reads the pair's left sequence,
/// only when it is needed.
fn state_of(
    m: &Manifest,
    spec: &KeySpec,
    right_now: RightManifest,
    built_from: impl FnOnce() -> Result<Option<u64>>,
) -> Result<JoinState> {
    let segs = pair_segments(m, spec);
    let Some(first) = segs.first() else {
        return Ok(JoinState::NotBuilt);
    };
    let pin = first.join.as_ref().ok_or_else(|| {
        Error::Other(format!(
            "corrupt index: the segment of {} pins no right manifest; rebuild it with \
             `bellwether index` on the left dataset",
            spec.identity()
        ))
    })?;
    let same = |p: &JoinPin| {
        p.right_sequence == pin.right_sequence
            && p.right_manifest == pin.right_manifest
            && p.left_live == pin.left_live
    };
    if segs.iter().any(|s| !s.join.as_ref().is_some_and(same)) {
        return Err(Error::Other(format!(
            "corrupt index: the segments of {} are pinned to different manifests; rebuild it \
             with `bellwether index` on the left dataset",
            spec.identity()
        )));
    }
    let Some(complete_at) = completeness(m, spec) else {
        return Ok(JoinState::LeftChanged {
            built_from: built_from()?.unwrap_or(0),
            current: m.sequence,
        });
    };
    let (pinned, current) = (pin.right_sequence, right_now.sequence);
    if pin.right_manifest != right_now.hash {
        return Ok(if current > pinned {
            JoinState::RightAdvanced { pinned, current }
        } else {
            JoinState::RightRebuilt { pinned, current }
        });
    }
    if pinned != current {
        return Err(Error::Other(format!(
            "corrupt index: the segments of {} pin the right manifest at sequence {pinned}, and \
             the manifest with that content is at sequence {current}; rebuild it with \
             `bellwether index` on the left dataset",
            spec.identity()
        )));
    }
    Ok(JoinState::Fresh {
        complete_at,
        right_sequence: pinned,
    })
}

impl<S: StoreExt + ?Sized> Planner<S> {
    /// The state of the declared join key `spec` in this (the left)
    /// manifest, against the right dataset's current manifest, which
    /// `right` pins.
    pub fn join_state<R: StoreExt + ?Sized>(
        &self,
        spec: &KeySpec,
        right: &Planner<R>,
    ) -> Result<JoinState> {
        state_of(
            self.manifest(),
            spec,
            RightManifest::of(&right.snapshot),
            || Ok(Some(pair_built_from(&self.snapshot, spec)?)),
        )
    }

    /// Opens the fresh join key `spec`'s pair segments against both
    /// datasets' schemas, refusing a stale key, a segment whose header
    /// pins other manifests than its manifest entry, and one whose recorded
    /// types differ from either dataset's.
    fn pair_readers<R: StoreExt + ?Sized>(
        &self,
        spec: &KeySpec,
        right: &Planner<R>,
    ) -> Result<Vec<SegmentReader<SegmentBytes<'_, S>>>> {
        let KeySpec::Join { right_columns, .. } = spec else {
            return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
        };
        match self.join_state(spec, right)? {
            JoinState::Fresh { .. } => {}
            other => {
                return Err(Error::KeySpec(format!(
                    "{} cannot be planned with: {}",
                    spec.name(),
                    other.describe()
                )))
            }
        }
        let want_right = column_types(right.manifest(), right_columns)?;
        let readers = pair_segments(self.manifest(), spec)
            .into_iter()
            .map(|s| {
                let r = SegmentReader::open_for_dataset(
                    SegmentBytes::Ranged {
                        snapshot: &self.snapshot,
                        id: s.id,
                        size: s.bytes,
                    },
                    &self.schema,
                )?;
                if r.key_spec() != spec {
                    return Err(Error::KeySpec(format!(
                        "a segment listed under {} indexes {}; restore the manifest from \
                         manifests/ or rebuild the spec",
                        spec.identity(),
                        r.key_spec().identity()
                    )));
                }
                let h = cj::header_pins(&r)?;
                let agrees = s.join.as_ref().is_some_and(|p| {
                    p.right_sequence == h.right_sequence
                        && p.right_manifest == h.right_manifest
                        && p.left_live == h.left_live
                });
                if !agrees {
                    return Err(Error::Other(format!(
                        "corrupt index: segment {} of {} pins other manifests in its header \
                         than in the manifest; rebuild it with `bellwether index` on the left \
                         dataset",
                        s.id,
                        spec.identity()
                    )));
                }
                let recorded = r.header().right_key_types.as_deref().unwrap_or_default();
                if recorded != want_right.as_slice() {
                    return Err(Error::KeySpec(format!(
                        "{} was built with right key types ({}), but the right dataset now \
                         declares ({}); re-index the left dataset to rebuild it",
                        spec.name(),
                        types_in_words(recorded),
                        types_in_words(&want_right)
                    )));
                }
                Ok(r)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(readers)
    }

    /// `semijoin(keys)` with the fresh join key `spec` (B3): `keys` are
    /// canonical tuples on its left columns (as [`Planner::encode_keys`]
    /// encodes them); `right` is the right dataset's planner, at the
    /// sequence the key is pinned to. A two-step plan: the left row groups
    /// holding a key both sides hold, then the right row groups holding
    /// one, each with an `In` residual over those keys. Invariant 1 holds on
    /// each side: no row of a matched key is outside its side's plan, and a
    /// key one side lacks has no row in the semi-join. A null probe matches
    /// nothing (ADR 014).
    pub fn plan_semijoin<R: StoreExt + ?Sized>(
        &self,
        spec: &KeySpec,
        keys: &[Vec<u8>],
        right: &Planner<R>,
    ) -> Result<SemiJoinPlan> {
        let KeySpec::Join {
            columns,
            right_columns,
            ..
        } = spec
        else {
            return Err(Error::KeySpec(format!("{} is not a join key", spec.name())));
        };
        let readers = self.pair_readers(spec, right)?;
        let mut asked: Vec<&Vec<u8>> = keys
            .iter()
            .filter(|k| bellwether_core::encoding::tuple_has_null(k) != Some(true))
            .collect();
        asked.sort_unstable();
        asked.dedup();
        let hits = cj::semijoin(&readers, keys)?;
        let (mut lsel, mut rsel) = (RoaringBitmap::new(), RoaringBitmap::new());
        let (mut lrows, mut rrows) = (Some(0u64), Some(0u64));
        let mut matched = Vec::with_capacity(hits.len());
        for (k, v) in hits {
            lsel |= &v.left.bitmap;
            rsel |= &v.right.bitmap;
            lrows = lrows
                .zip(v.left.counts.as_ref())
                .map(|(a, c)| a + c.iter().sum::<u64>());
            rrows = rrows
                .zip(v.right.counts.as_ref())
                .map(|(a, c)| a + c.iter().sum::<u64>());
            matched.push(k);
        }
        let counted = spec.counts();
        let mut left = self.finish(
            &lsel,
            Predicate::In {
                columns: columns.clone(),
                values: matched.clone(),
            },
        )?;
        let mut rplan = right.finish(
            &rsel,
            Predicate::In {
                columns: right_columns.clone(),
                values: matched.clone(),
            },
        )?;
        if counted {
            left.rows = lrows.map(Quantity::exact);
            rplan.rows = rrows.map(Quantity::exact);
        }
        Ok(SemiJoinPlan {
            keys: asked.len() as u64,
            matched: matched.len() as u64,
            left,
            right: rplan,
            right_sequence: right.sequence(),
        })
    }

    /// `overlap_stats()` of the fresh join key `spec` (B3), exact, from its
    /// pair index alone: `|L ∩ R|`, `|L \ R|`, `|R \ L|`, and the row
    /// groups (with counts, the rows) per shared key on each side.
    pub fn join_overlap<R: StoreExt + ?Sized>(
        &self,
        spec: &KeySpec,
        right: &Planner<R>,
    ) -> Result<OverlapStats> {
        let readers = self.pair_readers(spec, right)?;
        cj::overlap_stats(&readers)
    }
}
