//! [`Builder::verify`]: re-fingerprint every tracked file and report drift.

use std::io::ErrorKind;

use bellwether_source::{drift, fingerprint_with_policy, LocalFs};
use bellwether_store::{read_manifest, StoreExt};
use bellwether_types::{Error, FileFingerprint, Result};

use crate::build::Builder;
use crate::config::parse_policy;

/// A tracked file whose fingerprint no longer matches the manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Drift {
    /// The file's path as tracked.
    pub path: String,
    /// The fingerprint the manifest records.
    pub old: FileFingerprint,
    /// The fingerprint now, or `None` if the file is gone.
    pub new: Option<FileFingerprint>,
}

impl<S: StoreExt + ?Sized> Builder<S> {
    /// Re-fingerprints every tracked file under the policy the manifest
    /// records and returns the files that drifted, in manifest order:
    /// changed (size, etag or mtime, or content hash) or missing. Writes
    /// nothing. A changed file is re-indexed with fresh row-group ids by
    /// the next build (ADR 013).
    ///
    /// Only as strong as the policy: under the default `tail64k`, an
    /// in-place patch outside the last 64 KiB that keeps the size and mtime
    /// is not drift. `full` detects it.
    pub fn verify(&self) -> Result<Vec<Drift>> {
        let (m, _) = read_manifest(&**self.store(), self.index_root())?
            .ok_or_else(|| Error::Store(format!("no manifest under {:?}", self.index_root())))?;
        let policy = parse_policy(m.source.fingerprint_policy.as_deref())?;
        let mut out = Vec::new();
        for f in &m.rowgroup_space.files {
            let path = &f.fingerprint.path;
            let new = match std::fs::metadata(path) {
                Err(e) if e.kind() == ErrorKind::NotFound => None,
                _ => Some(fingerprint_with_policy(&LocalFs, path, policy)?),
            };
            if new.as_ref().is_none_or(|n| drift(&f.fingerprint, n)) {
                out.push(Drift {
                    path: path.clone(),
                    old: f.fingerprint.clone(),
                    new,
                });
            }
        }
        Ok(out)
    }
}
