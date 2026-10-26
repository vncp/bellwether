//! The index manifest, when the profiled folder has one.
//!
//! The profile reads it for two things: the declared key specs
//! (`declared_but_unused_key`) and the manifest sequence that goes into
//! `Provenance::manifest_version` (ADR 020). The manifest's content hash is
//! cited as a provenance input. An unindexed folder has no manifest, and its
//! Evidence carries `manifest_version: None` (invariant 6).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bellwether_store::{manifest_path, parse_rfc3339, Manifest, StoreExt};
use bellwether_types::{Error, Hash, KeySpec, Result};
use serde::{Deserialize, Serialize};

/// What the profile takes from a manifest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ManifestInfo {
    /// The manifest sequence (ADR 020).
    pub sequence: u64,
    /// BLAKE3 of the manifest bytes as stored.
    pub hash: Hash,
    /// RFC 3339 time of the commit that wrote it.
    pub committed_at: String,
    /// Source root the index was built over.
    pub source_root: String,
    /// Declared key specs.
    pub key_specs: Vec<KeySpec>,
    /// Segment bytes per key-spec name.
    pub segment_bytes: Vec<(String, u64)>,
}

/// Reads the manifest at the store root, with its age (time since its
/// commit, measured at `now_millis`, else the system clock), or `None` if
/// the folder is not indexed.
pub fn read<S: StoreExt + ?Sized>(
    store: &S,
    now_millis: Option<i64>,
) -> Result<Option<(ManifestInfo, Duration)>> {
    let Some((bytes, _)) = store.get_opt(&manifest_path(""))? else {
        return Ok(None);
    };
    let manifest = Manifest::from_slice(&bytes)?;
    let committed = if manifest.committed_at.is_empty() {
        &manifest.created_at
    } else {
        &manifest.committed_at
    };
    let millis = parse_rfc3339(committed)?.timestamp_millis();
    let now_millis = if let Some(n) = now_millis {
        n
    } else {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Other(format!("system clock before 1970: {e}")))?;
        i64::try_from(now.as_millis()).unwrap_or(i64::MAX)
    };
    let age = Duration::from_millis(u64::try_from(now_millis.saturating_sub(millis)).unwrap_or(0));
    let mut segment_bytes: Vec<(String, u64)> = Vec::new();
    for seg in &manifest.segments {
        match segment_bytes.iter_mut().find(|(n, _)| *n == seg.key_spec) {
            Some((_, b)) => *b += seg.bytes,
            None => segment_bytes.push((seg.key_spec.clone(), seg.bytes)),
        }
    }
    Ok(Some((
        ManifestInfo {
            sequence: manifest.sequence,
            hash: Hash::of(&bytes),
            committed_at: committed.clone(),
            source_root: manifest.source.root.clone(),
            key_specs: manifest.schema.key_specs.clone(),
            segment_bytes,
        },
        age,
    )))
}

/// Whether the manifest's source root names the profiled root: equal after
/// trimming trailing slashes, or the same directory on the local disk. An
/// empty source root (never recorded) matches anything.
#[must_use]
pub fn same_root(manifest_root: &str, root: &str) -> bool {
    let (a, b) = (
        manifest_root.trim_end_matches('/'),
        root.trim_end_matches('/'),
    );
    if a.is_empty() || a == b {
        return true;
    }
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(x), Ok(y)) => x == y,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roots_compare_by_name_or_directory() {
        assert!(same_root("", "/anything"));
        assert!(same_root("/data/orders/", "/data/orders"));
        assert!(!same_root("/data/orders", "/data/other"));
        let dir = std::env::temp_dir();
        let a = dir.to_string_lossy().to_string();
        assert!(same_root(&a, &format!("{a}/./")));
    }
}
