//! [`SidePlanner`] over the build crate's plan builder (WP4).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::datatypes::SchemaRef;
use bellwether_build::{Lookup, Planner};
use bellwether_store::{manifest_path, parse_rfc3339, Manifest, Snapshot, StoreExt};
use bellwether_types::{Error, FileFingerprint, Hash, ScanPlan};

use crate::planner::{ManifestIdentity, SidePlanner};

/// How many times [`IndexPlanner::open`] re-reads when a commit lands
/// between reading the manifest's bytes and pinning its snapshot.
const OPEN_ATTEMPTS: usize = 8;

/// An index built by `bellwether-build`, pinned at one manifest version and
/// used as a [`SidePlanner`].
///
/// The manifest's identity is its sequence (ADR 020) and the BLAKE3 of its
/// bytes as stored, the same content hash the profile cites; its age is the
/// time since its commit. Plans use every declared key spec, through
/// `Planner::plan(declared_specs, Lookup::Keys)`.
///
/// The build canonicalizes the source root it indexes, and the manifest's
/// fingerprints carry that spelling of each path. Reconcile the side with
/// [`IndexPlanner::source_root`] as its root, and with a source built the
/// way the index was (its dataset schema and fingerprint policy), or the
/// reconcile refuses the index as not describing the discovered files.
pub struct IndexPlanner<S: StoreExt + ?Sized> {
    planner: Planner<S>,
    hash: Hash,
}

impl<S: StoreExt + ?Sized> std::fmt::Debug for IndexPlanner<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexPlanner")
            .field("sequence", &self.planner.sequence())
            .field("hash", &self.hash)
            .finish_non_exhaustive()
    }
}

impl<S: StoreExt + ?Sized> IndexPlanner<S> {
    /// Pins the current manifest under `index_root`.
    pub fn open(store: &Arc<S>, index_root: &str) -> bellwether_types::Result<Self> {
        for _ in 0..OPEN_ATTEMPTS {
            let (bytes, _) = store.get(&manifest_path(index_root))?;
            let read = Manifest::from_slice(&bytes)?;
            let snapshot = Snapshot::open(Arc::clone(store), index_root)?;
            let pinned = snapshot.manifest();
            // One commit per sequence; the nonce guards against a store
            // that was reset and re-committed to the same sequence.
            if read.sequence == pinned.sequence && read.commit_nonce == pinned.commit_nonce {
                return Ok(Self {
                    planner: Planner::from_snapshot(snapshot)?,
                    hash: Hash::of(&bytes),
                });
            }
        }
        Err(Error::VersionConflict {
            expected: "a manifest stable while it is pinned".into(),
            found: format!("{OPEN_ATTEMPTS} commits in a row"),
        })
    }

    /// The source root the index was built over, as the manifest records it.
    #[must_use]
    pub fn source_root(&self) -> &str {
        &self.planner.manifest().source.root
    }

    /// The pinned manifest's sequence.
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.planner.sequence()
    }

    /// The dataset schema the manifest records; build the side's source
    /// with it.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        self.planner.schema()
    }

    fn age(&self) -> bellwether_types::Result<Duration> {
        let m = self.planner.manifest();
        let committed = if m.committed_at.is_empty() {
            &m.created_at
        } else {
            &m.committed_at
        };
        let at = parse_rfc3339(committed)?;
        let at_nanos = at
            .timestamp_nanos_opt()
            .ok_or_else(|| Error::Other(format!("commit time {committed} out of range")))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Other(format!("clock: {e}")))?;
        let now_nanos = i64::try_from(now.as_nanos()).unwrap_or(i64::MAX);
        // A commit stamped ahead of this clock (skew) is taken as fresh.
        Ok(Duration::from_nanos(
            u64::try_from(now_nanos.saturating_sub(at_nanos)).unwrap_or(0),
        ))
    }
}

impl<S: StoreExt + ?Sized + Send + Sync> SidePlanner for IndexPlanner<S> {
    fn manifest(&self) -> bellwether_types::Result<ManifestIdentity> {
        let m = self.planner.manifest();
        let files: Vec<FileFingerprint> = m
            .rowgroup_space
            .files
            .iter()
            .map(|f| f.fingerprint.clone())
            .collect();
        let rowgroup_ids = m
            .rowgroup_space
            .files
            .iter()
            .map(|f| f.rowgroup_ids.iter().map(|id| id.0).collect())
            .collect();
        let tombstoned = m
            .rowgroup_space
            .tombstones
            .iter()
            .flat_map(|t| t.rowgroup_ids.iter().map(|id| id.0))
            .collect();
        let chunks = m
            .rowgroup_space
            .files
            .iter()
            .map(|f| {
                f.chunks
                    .as_ref()
                    .map(|c| c.iter().map(|r| (r.start, r.end)).collect())
            })
            .collect();
        Ok(ManifestIdentity {
            sequence: m.sequence,
            hash: self.hash,
            files,
            rowgroup_ids,
            tombstoned,
            chunks,
            age: self.age()?,
            dataset_schema: SchemaRef::clone(self.planner.schema()),
        })
    }

    fn plan_keys(
        &self,
        columns: &[String],
        keys: &[Vec<u8>],
    ) -> bellwether_types::Result<ScanPlan> {
        self.planner.plan(
            // A point lookup reads no tombstone (B8): every spec plans as
            // the spec without one.
            &self
                .planner
                .declared_specs()
                .iter()
                .map(bellwether_types::KeySpec::without_tombstone)
                .collect::<Vec<_>>(),
            &Lookup::Keys {
                columns: columns.to_vec(),
                keys: keys.to_vec(),
            },
        )
    }
}
