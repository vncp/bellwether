//! The seam for an indexed side.
//!
//! A side may carry a [`SidePlanner`]: the index over its files. Then the
//! certificate is anchored to that index's manifest (`manifest_version` is
//! its sequence and the manifest's content hash is a provenance input), and
//! workload coverage reads only the row groups the index's plan selects for
//! the workload keys instead of scanning every row group.
//!
//! [`crate::IndexPlanner`] implements it over the build crate's plan
//! builder (`bellwether_build::Planner`, WP4); the trait stays the seam so a
//! caller can supply any other index, and tests a deliberately wrong one.

use std::time::Duration;

use arrow::datatypes::SchemaRef;
use bellwether_types::{FileFingerprint, Hash, ScanPlan};

/// The manifest an indexed side is planned against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestIdentity {
    /// The manifest sequence (ADR 020). It becomes the certificate's
    /// `manifest_version`.
    pub sequence: u64,
    /// The manifest's content hash, cited as a provenance input.
    pub hash: Hash,
    /// The live files the manifest tracks. The reconcile refuses to run if
    /// they differ from the files the side's source discovers: an index
    /// that does not describe the files would make the certificate's anchor
    /// meaningless and could hide rows from a plan.
    pub files: Vec<FileFingerprint>,
    /// The row-group ids the manifest assigned each file, in the order of
    /// `files`. A file whose source reports another number of row groups (a
    /// text source at another chunk size) is refused. With the source's
    /// reports and `tombstoned`, they make the side's catalog digest
    /// (`docs/format.md`, "Catalog digest").
    pub rowgroup_ids: Vec<Vec<u32>>,
    /// The row-group ids the manifest retired.
    pub tombstoned: Vec<u32>,
    /// The byte range `[start, end)` the manifest recorded for each row
    /// group of each file, in the order of `files`; `None` for formats with
    /// native row groups (Parquet). A tracked row group whose source
    /// reports another range (a text source at another chunk size, even
    /// one with the same count and lengths) is refused.
    pub chunks: Vec<Option<Vec<(u64, u64)>>>,
    /// The manifest's age when this identity was taken (from its commit
    /// time), for the certificate's `oldest_input_age`.
    pub age: Duration,
    /// The dataset schema the manifest records. Probes are encoded at the
    /// side's dataset types, so each key column must have the same canonical
    /// encoding (class, decimal scale, time unit) here; otherwise a probe
    /// could name a different value in the index and miss silently.
    pub dataset_schema: SchemaRef,
}

/// An index over one side's files, used through its plan builder.
///
/// Implementations are engine code: a plan must include every row group
/// holding a queried key (invariant 1). The reconcile cannot check that
/// without scanning, so it trusts the planner for completeness, and checks
/// everything else: that the manifest describes exactly the discovered files,
/// that the plan was computed against that manifest, and that every planned
/// row group exists.
pub trait SidePlanner: Send + Sync {
    /// The manifest plans are computed against. Called once per reconcile,
    /// before any plan.
    fn manifest(&self) -> bellwether_types::Result<ManifestIdentity>;

    /// A plan whose row groups include every row whose `columns` tuple is
    /// one of `keys` (canonical tuples at the dataset types, sorted and
    /// distinct). Its `manifest_version` must be the sequence `manifest()`
    /// returned.
    fn plan_keys(&self, columns: &[String], keys: &[Vec<u8>])
        -> bellwether_types::Result<ScanPlan>;
}
