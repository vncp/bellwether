//! Key subsumption and set-valued physical design (`docs/evolve.md` §5.2).
//!
//! Models when one [`KeySpec`] `a` subsumes another `b` (`subsumes(a, b)`),
//! meaning any lookup served by `b` is served at least as well by `a` without
//! needing `b` beside it:
//!
//! - Exact key with counts `exact:cols,counts` subsumes `exact:cols` without counts.
//! - Exact key with per-key time bounds `exact:cols@ts` (with or without counts)
//!   subsumes `exact:cols` (when counts are at least as strong).
//! - Composite exact key `exact:a,b` (with prefix seek, ADR 002) subsumes a
//!   leading prefix `exact:a` for point/locate pruning when `b` does not carry
//!   counts or bounds that `a` lacks (note: per-key counts on `a,b` do not
//!   answer `count` runs on `a` alone, because `counting_spec` requires exact
//!   column-set match; `subsumes_for_workloads` checks whether any run needs
//!   exact-column counts).
//! - Temporal key `temporal:cols@ts` subsumes `exact:cols` and `exact:cols@ts`
//!   (and with `counts: true`, subsumes `temporal:cols@ts` without counts and
//!   `exact:cols,counts`).
//! - Temporal key with tombstone `temporal:cols@ts,tombstone=t` subsumes
//!   `temporal:cols@ts` (its own segments are identical and it adds the
//!   tombstone sidecar), as well as `exact:cols`.
//! - Bitemporal key `temporal2:cols@valid,known,tombstone=t` subsumes
//!   `temporal2:cols@valid,known` without tombstone.
//! - Range key `range:ts` is subsumed by `exact:ts...` or `temporal:ts...@...`
//!   led by `ts` for window pruning.
//! - Same-name specs: an index holds at most one spec per [`KeySpec::name`],
//!   so two distinct specs with the same `name()` cannot coexist in one
//!   configuration `D`.

use bellwether_types::KeySpec;
use serde::{Deserialize, Serialize};

/// Why one [`KeySpec`] subsumes another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubsumptionKind {
    /// Identical key spec.
    Identical,
    /// Same key spec with per-key row counts added (`exact:a,counts` over `exact:a`).
    CountsAdded,
    /// Same key spec with per-key time bounds added (`exact:a@ts` over `exact:a`).
    BoundsAdded,
    /// Composite key whose leading prefix is the other key (`exact:a,b` over `exact:a`).
    PrefixSeek,
    /// Temporal key whose version chains also serve exact and window lookups on the same columns.
    TemporalCoversExact,
    /// Temporal or bitemporal key with a tombstone sidecar covers the same key without tombstone.
    TombstoneAdded,
    /// Key led by a timestamp column covers a `range:ts` key on that column.
    LeadingTimeCoversRange,
    /// Exact or temporal key covers a bloom filter on the same columns.
    ExactCoversBloom,
}

impl SubsumptionKind {
    /// Short snake_case label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Identical => "identical",
            Self::CountsAdded => "counts_added",
            Self::BoundsAdded => "bounds_added",
            Self::PrefixSeek => "prefix_seek",
            Self::TemporalCoversExact => "temporal_covers_exact",
            Self::TombstoneAdded => "tombstone_added",
            Self::LeadingTimeCoversRange => "leading_time_covers_range",
            Self::ExactCoversBloom => "exact_covers_bloom",
        }
    }
}

/// A recorded subsumption relationship (`by` subsumes `subsumed`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubsumptionPair {
    /// The dominated spec identity.
    pub subsumed: String,
    /// The dominating spec identity.
    pub by: String,
    /// How `by` subsumes `subsumed`.
    pub kind: SubsumptionKind,
}

/// Whether `a` subsumes `b` in general physical design (`docs/evolve.md` §5.2).
///
/// If `count_columns` lists column sets that require exact-match per-key counts
/// (for `count` or `aggregate` runs), a composite `exact:a,b,counts` does not
/// subsume `exact:a,counts` when `a` is in `count_columns`.
#[must_use]
pub fn subsumes(a: &KeySpec, b: &KeySpec) -> Option<SubsumptionKind> {
    subsumes_with_counts(a, b, &[])
}

/// Whether `a` subsumes `b`, preserving exact-column counts for any column set
/// in `count_columns`.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn subsumes_with_counts(
    a: &KeySpec,
    b: &KeySpec,
    count_columns: &[Vec<String>],
) -> Option<SubsumptionKind> {
    if a == b {
        return Some(SubsumptionKind::Identical);
    }
    let needs_exact_counts = |cols: &[String]| {
        count_columns
            .iter()
            .any(|c| c.len() == cols.len() && c.iter().all(|x| cols.contains(x)))
    };

    match (a, b) {
        // Exact vs Exact
        (
            KeySpec::Exact {
                columns: ca,
                counts: cnt_a,
                bounds: bnd_a,
            },
            KeySpec::Exact {
                columns: cb,
                counts: cnt_b,
                bounds: bnd_b,
            },
        ) => {
            if ca == cb {
                let counts_ok = *cnt_a || !*cnt_b;
                let bounds_ok = bnd_a == bnd_b || bnd_b.is_none();
                if counts_ok && bounds_ok {
                    if *cnt_a && !*cnt_b {
                        return Some(SubsumptionKind::CountsAdded);
                    }
                    if bnd_a.is_some() && bnd_b.is_none() {
                        return Some(SubsumptionKind::BoundsAdded);
                    }
                }
                return None;
            }
            // Prefix subsumption: ca starts with cb
            if cb.len() < ca.len() && ca[..cb.len()] == *cb {
                // Prefix seek works for locate, but does not provide per-key bounds
                // or exact-column counts for cb alone.
                if bnd_b.is_none() && (!*cnt_b || (!needs_exact_counts(cb) && *cnt_a)) {
                    return Some(SubsumptionKind::PrefixSeek);
                }
            }
            None
        }

        // Temporal vs Exact
        (
            KeySpec::Temporal {
                columns: ca,
                ts_column: ts_a,
                counts: cnt_a,
                ..
            },
            KeySpec::Exact {
                columns: cb,
                counts: cnt_b,
                bounds: bnd_b,
            },
        ) => {
            let bounds_ok = match bnd_b {
                None => true,
                Some(ts_b) => ts_a == ts_b,
            };
            if !bounds_ok {
                return None;
            }
            if ca == cb {
                if *cnt_a || !*cnt_b {
                    return Some(SubsumptionKind::TemporalCoversExact);
                }
            } else if cb.len() < ca.len()
                && ca[..cb.len()] == *cb
                && bnd_b.is_none()
                && (!*cnt_b || (!needs_exact_counts(cb) && *cnt_a))
            {
                return Some(SubsumptionKind::PrefixSeek);
            }
            None
        }

        // Exact or Temporal vs Bloom on same columns when bounds are covered
        (
            KeySpec::Exact {
                columns: ca,
                bounds: bnd_a,
                ..
            },
            KeySpec::Bloom {
                columns: cb,
                bounds: bnd_b,
                ..
            },
        ) if ca == cb && (bnd_b.is_none() || bnd_a == bnd_b) => {
            Some(SubsumptionKind::ExactCoversBloom)
        }
        (
            KeySpec::Temporal {
                columns: ca,
                ts_column,
                ..
            },
            KeySpec::Bloom {
                columns: cb,
                bounds: bnd_b,
                ..
            },
        ) if ca == cb && bnd_b.as_ref().is_none_or(|b| b == ts_column) => {
            Some(SubsumptionKind::ExactCoversBloom)
        }
        // Temporal vs Temporal
        (
            KeySpec::Temporal {
                columns: ca,
                ts_column: ts_a,
                counts: cnt_a,
                tombstone: tomb_a,
            },
            KeySpec::Temporal {
                columns: cb,
                ts_column: ts_b,
                counts: cnt_b,
                tombstone: tomb_b,
            },
        ) if ts_a == ts_b => {
            let counts_ok = *cnt_a || !*cnt_b;
            let tomb_ok = match (tomb_a, tomb_b) {
                (_, None) => true,
                (Some(ta), Some(tb)) => ta.same_test(tb),
                (None, Some(_)) => false,
            };
            if ca == cb && counts_ok && tomb_ok {
                if tomb_a.is_some() && tomb_b.is_none() {
                    return Some(SubsumptionKind::TombstoneAdded);
                }
                if *cnt_a && !*cnt_b {
                    return Some(SubsumptionKind::CountsAdded);
                }
            }
            None
        }

        // Bitemporal vs Bitemporal
        (
            KeySpec::Bitemporal {
                columns: ca,
                valid_column: va,
                known_column: ka,
                tombstone: tomb_a,
            },
            KeySpec::Bitemporal {
                columns: cb,
                valid_column: vb,
                known_column: kb,
                tombstone: tomb_b,
            },
        ) if ca == cb && va == vb && ka == kb => {
            if tomb_a.is_some() && tomb_b.is_none() {
                return Some(SubsumptionKind::TombstoneAdded);
            }
            None
        }

        // Exact or Temporal led by ts covers Range { column: ts } for window pruning
        (
            KeySpec::Exact { columns, .. } | KeySpec::Temporal { columns, .. },
            KeySpec::Range { column },
        ) if columns.first().is_some_and(|c| c == column) => {
            Some(SubsumptionKind::LeadingTimeCoversRange)
        }

        _ => None,
    }
}

/// Whether `candidate` is redundant (subsumed by an existing member, or shares
/// a slot `KeySpec::name()` with an existing member) in `set`.
#[must_use]
pub fn is_redundant_in_set(
    candidate: &KeySpec,
    set: &[KeySpec],
    count_columns: &[Vec<String>],
) -> bool {
    set.iter().any(|existing| {
        existing == candidate
            || subsumes_with_counts(existing, candidate, count_columns).is_some()
            || existing.name() == candidate.name()
    })
}

/// Removes any spec in `specs` that is subsumed by another spec in `specs`,
/// and ensures all remaining specs have distinct [`KeySpec::name`]s (keeping
/// the dominating or earlier spec). Returns the pruned set and the removed
/// subsumption pairs.
#[must_use]
pub fn canonicalize_set(
    specs: &[KeySpec],
    count_columns: &[Vec<String>],
) -> (Vec<KeySpec>, Vec<SubsumptionPair>) {
    let mut kept: Vec<KeySpec> = Vec::new();
    let mut removed: Vec<SubsumptionPair> = Vec::new();

    for (i, cand) in specs.iter().enumerate() {
        let mut dominated_by: Option<(String, SubsumptionKind)> = None;
        for (j, other) in specs.iter().enumerate() {
            if i == j {
                continue;
            }
            if let Some(kind) = subsumes_with_counts(other, cand, count_columns) {
                if kind == SubsumptionKind::Identical && j > i {
                    continue;
                }
                dominated_by = Some((other.identity(), kind));
                break;
            }
        }
        if let Some((by, kind)) = dominated_by {
            if kind != SubsumptionKind::Identical {
                removed.push(SubsumptionPair {
                    subsumed: cand.identity(),
                    by,
                    kind,
                });
            }
            continue;
        }
        if !kept.iter().any(|k| k.name() == cand.name()) {
            kept.push(cand.clone());
        }
    }
    (kept, removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bellwether_types::Tombstone;

    fn cols(c: &[&str]) -> Vec<String> {
        c.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn exact_counts_and_bounds_subsumption() {
        let base = KeySpec::Exact {
            columns: cols(&["a"]),
            counts: false,
            bounds: None,
        };
        let counted = KeySpec::Exact {
            columns: cols(&["a"]),
            counts: true,
            bounds: None,
        };
        let bounded = KeySpec::Exact {
            columns: cols(&["a"]),
            counts: false,
            bounds: Some("ts".into()),
        };
        let both = KeySpec::Exact {
            columns: cols(&["a"]),
            counts: true,
            bounds: Some("ts".into()),
        };
        assert_eq!(
            subsumes(&counted, &base),
            Some(SubsumptionKind::CountsAdded)
        );
        assert_eq!(subsumes(&base, &counted), None);
        assert_eq!(
            subsumes(&bounded, &base),
            Some(SubsumptionKind::BoundsAdded)
        );
        assert_eq!(
            subsumes(&both, &counted),
            Some(SubsumptionKind::BoundsAdded)
        );
        assert_eq!(
            subsumes(&both, &bounded),
            Some(SubsumptionKind::CountsAdded)
        );
    }

    #[test]
    fn composite_prefix_subsumes_leading_singleton() {
        let single = KeySpec::Exact {
            columns: cols(&["a"]),
            counts: false,
            bounds: None,
        };
        let comp = KeySpec::Exact {
            columns: cols(&["a", "b"]),
            counts: false,
            bounds: None,
        };
        let other_order = KeySpec::Exact {
            columns: cols(&["b", "a"]),
            counts: false,
            bounds: None,
        };
        assert_eq!(subsumes(&comp, &single), Some(SubsumptionKind::PrefixSeek));
        assert_eq!(subsumes(&single, &comp), None);
        assert_eq!(subsumes(&other_order, &single), None);
    }

    #[test]
    fn temporal_subsumes_exact_and_tombstone_subsumes_temporal() {
        let exact = KeySpec::Exact {
            columns: cols(&["id"]),
            counts: false,
            bounds: None,
        };
        let temp = KeySpec::Temporal {
            columns: cols(&["id"]),
            ts_column: "ts".into(),
            counts: false,
            tombstone: None,
        };
        let temp_tomb = KeySpec::Temporal {
            columns: cols(&["id"]),
            ts_column: "ts".into(),
            counts: false,
            tombstone: Some(Tombstone {
                column: "void".into(),
                value: None,
            }),
        };
        assert_eq!(
            subsumes(&temp, &exact),
            Some(SubsumptionKind::TemporalCoversExact)
        );
        assert_eq!(
            subsumes(&temp_tomb, &temp),
            Some(SubsumptionKind::TombstoneAdded)
        );
        assert_eq!(
            subsumes(&temp_tomb, &exact),
            Some(SubsumptionKind::TemporalCoversExact)
        );
    }

    #[test]
    fn canonicalize_removes_dominated_specs() {
        let s1 = KeySpec::Exact {
            columns: cols(&["a"]),
            counts: false,
            bounds: None,
        };
        let s2 = KeySpec::Exact {
            columns: cols(&["a", "b"]),
            counts: false,
            bounds: None,
        };
        let (kept, removed) = canonicalize_set(&[s1, s2.clone()], &[]);
        assert_eq!(kept, vec![s2]);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].subsumed, "exact:a");
        assert_eq!(removed[0].by, "exact:a,b");
        assert_eq!(removed[0].kind, SubsumptionKind::PrefixSeek);
    }
}
