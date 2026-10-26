//! Recorded-workload summary for `EvidenceBody::Profile.workload_keys`.
//!
//! The only recorded-workload format that exists today is the benchmark's
//! (`bellwether-bench-workload/1`): a directory holding `workload.json` and
//! one Parquet file of keys per run. Each run file has one `UInt64` key
//! column; a run's hash is BLAKE3 over its sorted, deduplicated keys as
//! 8-byte big-endian values, and the workload hash is BLAKE3 over the run
//! hashes in listed order. WP6 (`bellwether-replay`) will own the recorder
//! and may define its own format; this reader accepts exactly the bench
//! format and refuses anything else.
//!
//! Nothing in `workload.json` is trusted: every run's keys are read back,
//! counted, and hashed, and the run hashes and the workload hash must match
//! what the file claims. A mismatch is an error, not a best guess.
//!
//! A run may say which lookup it was (`query`: `locate`, the default, or
//! `count`, the row count of its keys). The summary counts the count runs
//! that precede a later run over the same keys (same run hash), the input of
//! the `R-counts-1` detector (`crate::lints::count_before_scan`).
//!
//! Memory: one run's key set at a time (ADR 018, "Replay holds one run's
//! key set").

use std::path::{Component, Path, PathBuf};

use arrow::array::{Array, UInt64Array};
use bellwether_source::{fingerprint, ParquetSource};
use bellwether_types::{Error, FileFingerprint, Hash, Result, Source};
use serde::{Deserialize, Serialize};

use crate::quantile::p50_p95_max_u64;

/// The workload format this reader accepts.
pub const BENCH_WORKLOAD_FORMAT: &str = "bellwether-bench-workload/1";
/// The manifest file name inside a workload directory.
pub const WORKLOAD_FILE: &str = "workload.json";
/// Format tag of the cached summary. Part of the cache key.
pub const WORKLOAD_CACHE_FORMAT: &str = "bellwether-profile/workload-summary/2";
/// A run's `query` when it looks up every row of its keys (the default).
pub const QUERY_LOCATE: &str = "locate";
/// A run's `query` when it counts the rows of its keys.
pub const QUERY_COUNT: &str = "count";

/// What a recorded workload looks like, for the profile.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSummary {
    /// The workload hash, verified against the run files.
    pub hash: Hash,
    /// Recorded runs.
    pub runs: u64,
    /// Every column a run's key set is on: the columns the workload looks
    /// up by.
    pub key_columns: Vec<String>,
    /// Distinct keys per run at nearest-rank p50, p95, and max.
    pub sizes: [u64; 3],
    /// Runs with `query: "count"` that a later run over the same keys (the
    /// same run hash) follows: a count pass ahead of the query it gates.
    /// Zero (omitted in JSON) when no count run precedes one.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub counts_before_scan: u64,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde passes a reference
fn is_zero(n: &u64) -> bool {
    *n == 0
}

#[derive(Deserialize)]
struct BenchWorkload {
    format: String,
    runs: Vec<BenchRun>,
    hash: Hash,
}

#[derive(Deserialize)]
struct BenchRun {
    run_id: String,
    file: String,
    keys: u64,
    hash: Hash,
    #[serde(default)]
    query: Option<String>,
}

/// The directory and manifest path of a workload given as either.
fn locate(path: &Path) -> (PathBuf, PathBuf) {
    if path.is_dir() {
        (path.to_path_buf(), path.join(WORKLOAD_FILE))
    } else {
        let dir = path
            .parent()
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
        (dir, path.to_path_buf())
    }
}

/// A run file path inside the workload directory. Absolute paths and `..`
/// are refused: a run must live under its workload.
fn run_path(dir: &Path, file: &str) -> Result<PathBuf> {
    let rel = Path::new(file);
    if rel.is_absolute()
        || rel
            .components()
            .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
    {
        return Err(Error::Other(format!(
            "workload run file {file:?} must be a relative path inside the workload directory"
        )));
    }
    Ok(dir.join(rel))
}

fn read_manifest(path: &Path) -> Result<(Vec<u8>, BenchWorkload)> {
    let bytes =
        std::fs::read(path).map_err(|e| Error::Source(format!("{}: {e}", path.display())))?;
    let parsed: BenchWorkload = serde_json::from_slice(&bytes)?;
    if parsed.format != BENCH_WORKLOAD_FORMAT {
        return Err(Error::Other(format!(
            "{}: workload format {:?} is not supported; expected {BENCH_WORKLOAD_FORMAT:?}",
            path.display(),
            parsed.format
        )));
    }
    Ok((bytes, parsed))
}

fn path_str(p: &Path) -> Result<&str> {
    p.to_str()
        .ok_or_else(|| Error::Source(format!("{}: path is not UTF-8", p.display())))
}

/// The cache key inputs of a workload: its manifest bytes and every run
/// file's fingerprint. Any change to either changes the key.
#[derive(Serialize)]
pub struct WorkloadKey {
    format: &'static str,
    manifest_blake3: Hash,
    runs: Vec<FileFingerprint>,
}

/// Computes the cache key inputs without reading any key.
pub fn cache_key(path: &Path) -> Result<WorkloadKey> {
    let (dir, manifest) = locate(path);
    let (bytes, parsed) = read_manifest(&manifest)?;
    let runs = parsed
        .runs
        .iter()
        .map(|r| fingerprint(path_str(&run_path(&dir, &r.file)?)?))
        .collect::<Result<Vec<_>>>()?;
    Ok(WorkloadKey {
        format: WORKLOAD_CACHE_FORMAT,
        manifest_blake3: Hash::of(&bytes),
        runs,
    })
}

/// Age of the workload manifest (time since it was last written).
pub fn age(path: &Path) -> Result<std::time::Duration> {
    let (_, manifest) = locate(path);
    let modified = std::fs::metadata(&manifest)
        .and_then(|m| m.modified())
        .map_err(|e| Error::Source(format!("{}: {e}", manifest.display())))?;
    Ok(std::time::SystemTime::now()
        .duration_since(modified)
        .unwrap_or_default())
}

/// Reads and verifies a bench-format workload.
pub fn summarize(path: &Path) -> Result<WorkloadSummary> {
    let (dir, manifest) = locate(path);
    let (_, parsed) = read_manifest(&manifest)?;
    if parsed.runs.is_empty() {
        return Err(Error::Other(format!(
            "{}: no recorded runs",
            manifest.display()
        )));
    }
    let source = ParquetSource::new();
    let mut sizes = Vec::with_capacity(parsed.runs.len());
    let mut key_columns: Vec<String> = Vec::new();
    let mut workload_hasher = blake3::Hasher::new();
    // Per run, in listed order: whether it counts, and its verified hash.
    let mut runs: Vec<(bool, Hash)> = Vec::with_capacity(parsed.runs.len());
    for run in &parsed.runs {
        let counts = match run.query.as_deref() {
            None | Some(QUERY_LOCATE) => false,
            Some(QUERY_COUNT) => true,
            Some(other) => {
                return Err(Error::Other(format!(
                    "{}: run {} has query {other:?}; a bench run is a {QUERY_LOCATE:?} or a \
                     {QUERY_COUNT:?}",
                    manifest.display(),
                    run.run_id
                )))
            }
        };
        let file = run_path(&dir, &run.file)?;
        let fp = fingerprint(path_str(&file)?)?;
        let mut keys: Vec<u64> = Vec::new();
        for rg in source.row_groups(&fp)? {
            let columns: Vec<String> = rg.stats.iter().map(|s| s.column.clone()).collect();
            if columns.len() != 1 {
                return Err(Error::Other(format!(
                    "{}: a bench run file has one key column, found {columns:?}",
                    file.display()
                )));
            }
            for c in &columns {
                if !key_columns.contains(c) {
                    key_columns.push(c.clone());
                }
            }
            let batch = source.read_columns(&rg, &columns)?;
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    Error::Other(format!(
                        "{}: bench run keys must be uint64, found {}",
                        file.display(),
                        batch.column(0).data_type()
                    ))
                })?;
            if col.null_count() > 0 {
                return Err(Error::Other(format!(
                    "{}: null key in a run",
                    file.display()
                )));
            }
            keys.extend(col.values().iter().copied());
        }
        keys.sort_unstable();
        keys.dedup();
        let mut h = blake3::Hasher::new();
        for k in &keys {
            h.update(&k.to_be_bytes());
        }
        let run_hash = Hash(*h.finalize().as_bytes());
        if run_hash != run.hash || keys.len() as u64 != run.keys {
            return Err(Error::Other(format!(
                "{}: run {} holds {} keys hashing to {}, but {} records {} keys hashing to {}",
                file.display(),
                run.run_id,
                keys.len(),
                run_hash,
                WORKLOAD_FILE,
                run.keys,
                run.hash
            )));
        }
        workload_hasher.update(&run_hash.0);
        sizes.push(keys.len() as u64);
        runs.push((counts, run_hash));
    }
    let hash = Hash(*workload_hasher.finalize().as_bytes());
    if hash != parsed.hash {
        return Err(Error::Other(format!(
            "{}: run hashes combine to {hash}, but the file records {}",
            manifest.display(),
            parsed.hash
        )));
    }
    sizes.sort_unstable();
    // Walk backwards, remembering the hashes of the non-count runs seen so
    // far: a count run whose hash is among them precedes a run over its keys.
    let mut later_scans: std::collections::HashSet<Hash> = std::collections::HashSet::new();
    let mut counts_before_scan = 0u64;
    for (counts, h) in runs.iter().rev() {
        if !counts {
            later_scans.insert(*h);
        } else if later_scans.contains(h) {
            counts_before_scan += 1;
        }
    }
    Ok(WorkloadSummary {
        hash,
        runs: sizes.len() as u64,
        key_columns,
        sizes: p50_p95_max_u64(&sizes),
        counts_before_scan,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_paths_stay_inside_the_workload() {
        let dir = Path::new("/w");
        assert_eq!(
            run_path(dir, "workload/run-0000.parquet").unwrap(),
            Path::new("/w/workload/run-0000.parquet")
        );
        assert!(run_path(dir, "../etc/passwd").is_err());
        assert!(run_path(dir, "/etc/passwd").is_err());
        assert!(run_path(dir, "a/../../b").is_err());
    }
}
