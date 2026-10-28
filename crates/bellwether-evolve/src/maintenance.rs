//! Maintenance cost `m(D)` for physical design configurations (`docs/evolve.md` §3, §5.2, §6).
//!
//! For each [`KeySpec`] in a candidate configuration `D`, computes:
//! - exact segment bytes (when the spec is already built in the committed index or
//!   built in `Replayer`'s transient cache), and
//! - analytical profile-based byte estimate (`rows * (-ln(fpr) / (ln 2)^2) / 8` for
//!   bloom filters; distinct keys × per-entry key/bitmap/count/bounds overhead for
//!   exact, temporal, bitemporal, range, list, text, and agg keys), and
//! - row groups and rows scanned to build the new specs in `D` (union of columns
//!   across new specs in `D`, scanned once per live row group).

use bellwether_profile::ProfileReport;
use bellwether_replay::Replayer;
use bellwether_types::KeySpec;
use serde::{Deserialize, Serialize};

use crate::knobs::bloom_bytes_estimate;

/// Maintenance cost of a single [`KeySpec`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpecMaintenance {
    /// Full identity of the key spec ([`KeySpec::identity`]).
    pub spec: String,
    /// Segment bytes: exact from transient/committed segments when built, else
    /// profile-estimated.
    pub segment_bytes: u64,
    /// Analytical estimate from profile statistics / formula.
    pub estimated_bytes: u64,
    /// Whether `segment_bytes` comes from actual built segments (`true`) or the
    /// analytical estimate (`false`).
    pub exact_bytes: bool,
    /// Whether this spec is already complete in the current manifest (0 build cost).
    pub already_built: bool,
    /// Row groups scanned to build this spec (0 when `already_built`).
    pub build_row_groups: u64,
    /// Rows scanned to build this spec (0 when `already_built`).
    pub build_rows: u64,
}

/// Maintenance cost `m(D)` of a candidate configuration `D` (`Vec<KeySpec>`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceCost {
    /// Total index segment bytes across all specs in `D`.
    pub index_bytes: u64,
    /// Newly added index segment bytes beyond the current configuration.
    pub added_index_bytes: u64,
    /// Analytical estimated bytes across all specs in `D`.
    pub estimated_index_bytes: u64,
    /// Number of key specs in `D`.
    pub spec_count: usize,
    /// Row groups scanned to build the new specs in `D`.
    pub build_row_groups: u64,
    /// Rows scanned to build the new specs in `D`.
    pub build_rows: u64,
    /// Distinct columns projected across all new specs in `D`.
    pub build_columns: Vec<String>,
    /// Per-spec breakdown.
    pub specs: Vec<SpecMaintenance>,
}

/// Analytical estimate of segment bytes for `spec` from the [`ProfileReport`].
#[must_use]
pub fn analytical_spec_bytes(spec: &KeySpec, profile: &ProfileReport) -> u64 {
    let rows = profile.rows_total.max(1);
    let rgs = profile.row_groups_total.max(1);
    let distinct = profile
        .fanout
        .iter()
        .find(|f| f.fanout.columns == spec.columns())
        .or_else(|| {
            spec.columns().first().and_then(|c| {
                profile
                    .fanout
                    .iter()
                    .find(|f| f.fanout.columns.len() == 1 && &f.fanout.columns[0] == c)
            })
        })
        .map_or_else(|| (rows / 4).max(1), |f| f.fanout.distinct.max(1));

    match spec {
        KeySpec::Bloom { fpr, bounds, .. } => {
            let filter_bytes = bloom_bytes_estimate(rows, *fpr).saturating_add(64);
            let bounds_bytes = if bounds.is_some() {
                distinct.saturating_mul(24)
            } else {
                0
            };
            filter_bytes.saturating_add(bounds_bytes)
        }
        KeySpec::Range { .. } => rgs.saturating_mul(32).saturating_add(64),
        KeySpec::Exact { counts, bounds, .. } => {
            let mut per_key = 24u64;
            if *counts {
                per_key = per_key.saturating_add(8);
            }
            if bounds.is_some() {
                per_key = per_key.saturating_add(16);
            }
            distinct.saturating_mul(per_key).saturating_add(64)
        }
        KeySpec::Temporal {
            counts, tombstone, ..
        } => {
            let mut per_key = 36u64;
            if *counts {
                per_key = per_key.saturating_add(8);
            }
            if tombstone.is_some() {
                per_key = per_key.saturating_add(17);
            }
            distinct.saturating_mul(per_key).saturating_add(64)
        }
        KeySpec::Bitemporal { tombstone, .. } => {
            let mut per_key = 48u64;
            if tombstone.is_some() {
                per_key = per_key.saturating_add(33);
            }
            distinct.saturating_mul(per_key).saturating_add(64)
        }
        KeySpec::List { .. } | KeySpec::Text { .. } => {
            distinct.saturating_mul(32).saturating_add(64)
        }
        KeySpec::Agg { functions, .. } => distinct
            .saturating_mul(24 + (functions.len() as u64) * 16)
            .saturating_add(64),
        KeySpec::Expr { over, .. } => analytical_spec_bytes(over, profile),
        KeySpec::Join { counts, .. } => {
            let per_key = if *counts { 40 } else { 24 };
            distinct.saturating_mul(per_key).saturating_add(64)
        }
    }
}

/// Computes the [`MaintenanceCost`] `m(D)` for `specs` using exact segment sizes
/// from `replayer` when available (building or reading cached transient segments)
/// and analytical profile estimates alongside them.
pub fn estimate_maintenance(
    specs: &[KeySpec],
    replayer: &Replayer<'_>,
    profile: &ProfileReport,
) -> MaintenanceCost {
    let current = replayer.current_specs();
    let total_rgs = replayer.catalog().len() as u64;
    let total_rows = replayer.catalog().rows_total();

    let mut per_spec = Vec::with_capacity(specs.len());
    let mut index_bytes = 0u64;
    let mut added_index_bytes = 0u64;
    let mut estimated_index_bytes = 0u64;
    let mut build_columns: Vec<String> = Vec::new();
    let mut any_new = false;

    for spec in specs {
        let already_built = current.contains(spec);
        let est = analytical_spec_bytes(spec, profile);
        let (seg_bytes, exact_bytes, b_rgs, b_rows, cols) = if matches!(spec, KeySpec::Join { .. })
        {
            (
                est,
                false,
                if already_built { 0 } else { total_rgs },
                if already_built { 0 } else { total_rows },
                spec.columns().to_vec(),
            )
        } else if let Ok(transient) = replayer.transient(spec) {
            let st = transient.stats();
            (
                st.bytes,
                true,
                if already_built { 0 } else { st.row_groups_read },
                if already_built { 0 } else { st.rows_read },
                st.columns_read.clone(),
            )
        } else {
            (
                est,
                false,
                if already_built { 0 } else { total_rgs },
                if already_built { 0 } else { total_rows },
                spec.columns().to_vec(),
            )
        };

        index_bytes = index_bytes.saturating_add(seg_bytes);
        if !already_built {
            added_index_bytes = added_index_bytes.saturating_add(seg_bytes);
            any_new = true;
            for c in cols {
                if !build_columns.contains(&c) {
                    build_columns.push(c);
                }
            }
        }
        estimated_index_bytes = estimated_index_bytes.saturating_add(est);

        per_spec.push(SpecMaintenance {
            spec: spec.identity(),
            segment_bytes: seg_bytes,
            estimated_bytes: est,
            exact_bytes,
            already_built,
            build_row_groups: b_rgs,
            build_rows: b_rows,
        });
    }

    build_columns.sort();
    MaintenanceCost {
        index_bytes,
        added_index_bytes,
        estimated_index_bytes,
        spec_count: specs.len(),
        build_row_groups: if any_new { total_rgs } else { 0 },
        build_rows: if any_new { total_rows } else { 0 },
        build_columns,
        specs: per_spec,
    }
}
