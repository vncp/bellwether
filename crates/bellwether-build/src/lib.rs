//! Index builder for Bellwether (WP4).
//!
//! Discover → fingerprint → assign row-group ids → parallel segments → one
//! manifest commit; incremental builds, compaction, verification, and scan
//! plans. Owns ADR 012 (key-spec completeness is a manifest fact) and
//! ADR 013 (fingerprint drift retires row-group ids, which are never
//! reused).
//!
//! - [`Builder::build`] makes the index declare the given key specs, in
//!   addition to those it already declares, over the files under a
//!   source folder, in one commit, indexing only what is not covered yet;
//!   [`Builder::remove_spec`] removes one explicitly. Sources are always built with the
//!   dataset schema, the index's fingerprint policy, and, for text, the
//!   chunk size each file records.
//! - [`Builder::compact`] merges each spec's segments with core's merge.
//! - [`Builder::verify`] reports fingerprint drift without writing.
//! - [`completeness`] says whether a spec covers every live row group.
//! - [`Planner`] turns key specs and a [`Lookup`] into a
//!   [`bellwether_types::ScanPlan`] with exact bytes and a residual
//!   predicate, against one pinned manifest version.
//!
//! Memory (ADR 018): a build worker holds one row group's key columns and
//! one capped open segment per spec it builds (see [`BuildOptions`]); a
//! plan holds block indexes, the blocks it touches, one whole bloom or range
//! segment at a time, and the bitmaps for the keys asked.

#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

mod build;
mod compact;
mod config;
mod coverage;
mod keys;
mod plan;
mod seg_io;
mod segments;
mod sources;
mod target;
mod verify;

pub use build::{BuildReport, Builder, TextTablesReplaced, MAX_KEY_SPECS};
pub use compact::CompactReport;
pub use config::{
    BuildObserver, BuildOptions, CompactOptions, SourceKind, DEFAULT_MAX_RESTARTS,
    DEFAULT_MAX_SEGMENT_BYTES, DEFAULT_MAX_SEGMENT_KEYS, DEFAULT_MAX_SEGMENT_ROW_GROUPS,
};
pub use coverage::{completeness, sidecar_completeness, sketch_completeness};
pub use keys::encode_ts;
pub use plan::{
    default_window_column, overlapping_counts, partition_columns, KeyCount, Lookup, PlanOrder,
    Planner, TransientSpec,
};
pub use target::declared_sketches;
pub use verify::Drift;
