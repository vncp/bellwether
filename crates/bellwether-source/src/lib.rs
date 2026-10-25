//! Sources for the Bellwether engine (WP2).
//!
//! - [`ParquetSource`]: real Parquet row groups, footer statistics encoded
//!   canonically, projected column reads, embedded bloom filters reported.
//! - [`ChunkedTextSource`]: CSV and JSON lines split into newline-aligned
//!   byte-range chunks that stand in for row groups.
//! - [`fingerprint()`] and [`drift`]: file identity per `docs/format.md`
//!   (ADR 013).
//!
//! Memory bound (ADR 018): `row_groups` holds one footer (Parquet) or one
//! chunk's bytes plus one decoded batch (text). `read_columns` holds one row
//! group's projected columns. Chunks are capped at 4× the chunk size.
//! Metadata caches are bounded by bytes. Nothing holds a whole file in
//! memory. [`FingerprintPolicy::Full`] streams the whole file through BLAKE3
//! in bounded windows; the default policy reads only the last 64 KiB.
//!
//! Every footer or layout load, and every `read_columns`, verifies the
//! caller's fingerprint on the **opened handle**: size, etag/mtime, and the
//! content hash chosen by [`FingerprintPolicy`] must all match, and every
//! read then goes through that handle. This catches a stale cache, a
//! rewrite that changes any fingerprint component, and a rename-replace
//! between check and read. It has two inherent limits (ADR 013):
//!
//! - **Tail coverage.** Under the default [`FingerprintPolicy::Tail64K`], the
//!   content hash covers only the last 64 KiB. An in-place patch outside it
//!   that keeps the size and the mtime/etag goes undetected, and the patched
//!   bytes are served. [`FingerprintPolicy::Full`] closes this for small or
//!   critical files, at the cost of hashing the whole file per verification.
//! - **Inode, not bytes.** Verifying the opened handle pins the inode (or
//!   object version), not its bytes. An in-place write to the same inode
//!   during a read goes undetected under either policy.
//!
//! Byte access goes through [`Storage`]. Only [`LocalFs`] is implemented; an
//! object-store backend plugs in later without touching the sources.

#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod canonical;
pub mod catalog;
pub mod fingerprint;
pub mod parquet_source;
pub mod partition;
pub mod stats;
pub mod storage;
pub mod text;

use std::collections::{HashMap, VecDeque};
use std::hash::Hash as StdHash;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use bellwether_types::{Error, FileFingerprint, Result};

pub use canonical::{Canonical, DefaultCanonical};
use fingerprint::content_hash;
pub use fingerprint::{
    drift, fingerprint, fingerprint_with, fingerprint_with_policy, FingerprintPolicy, TAIL_BYTES,
};
pub use parquet_source::{load_reader_metadata, ParquetSource};
pub use partition::{Partitioning, PathTemplate};
pub use storage::{LocalFs, ObjectMeta, Storage};
pub use text::{ChunkedTextSource, TextFormat, DEFAULT_CHUNK_SIZE};

/// Opens `file` and verifies, on the opened handle, that it still matches
/// `file`: size, etag/mtime, and content hash under `policy`. Returns the
/// handle so every subsequent read hits the verified inode.
pub(crate) fn open_verified<S: Storage>(
    storage: &S,
    file: &FileFingerprint,
    policy: FingerprintPolicy,
) -> Result<S::Reader> {
    let (reader, now) = storage.open(&file.path)?;
    let drift_err = |what: &str| {
        Error::Source(format!(
            "{}: fingerprint drift ({what}); re-discover the file",
            file.path
        ))
    };
    if now.size != file.size {
        return Err(drift_err(&format!("size {} -> {}", file.size, now.size)));
    }
    if now.etag != file.etag {
        return Err(drift_err(&format!(
            "etag {:?} -> {:?}",
            file.etag, now.etag
        )));
    }
    if content_hash(&reader, &file.path, now.size, policy)? != file.tail_blake3 {
        return Err(drift_err(&format!("{policy:?} content hash")));
    }
    Ok(reader)
}

/// Indices into `schema` for `columns`, sorted and deduplicated. Unknown
/// names are a [`Error::KeySpec`] error.
pub(crate) fn resolve_columns(schema: &Schema, columns: &[String]) -> Result<Vec<usize>> {
    let mut idx = columns
        .iter()
        .map(|c| {
            schema
                .index_of(c)
                .map_err(|_| Error::KeySpec(format!("column {c:?} not in source schema")))
        })
        .collect::<Result<Vec<_>>>()?;
    idx.sort_unstable();
    idx.dedup();
    Ok(idx)
}

/// `batch` with exactly `columns`, in that order.
pub(crate) fn reorder(batch: &RecordBatch, columns: &[String]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let idx = columns
        .iter()
        .map(|c| {
            schema
                .index_of(c)
                .map_err(|_| Error::KeySpec(format!("column {c:?} not in decoded batch")))
        })
        .collect::<Result<Vec<_>>>()?;
    batch
        .project(&idx)
        .map_err(|e| Error::Source(format!("projection: {e}")))
}

/// A FIFO cache bounded by the summed estimated size of its values.
#[derive(Debug)]
pub(crate) struct BoundedCache<K, V> {
    map: HashMap<K, (V, usize)>,
    order: VecDeque<K>,
    bytes: usize,
    capacity: usize,
}

impl<K: Eq + StdHash + Clone, V: Clone> BoundedCache<K, V> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            capacity,
        }
    }

    pub(crate) fn get(&self, k: &K) -> Option<V> {
        self.map.get(k).map(|(v, _)| v.clone())
    }

    /// Inserts `v` costing `size` bytes, evicting oldest entries as needed.
    /// A value larger than the whole capacity is not cached.
    pub(crate) fn insert(&mut self, k: K, v: V, size: usize) {
        if size > self.capacity {
            return;
        }
        if let Some((_, old)) = self.map.remove(&k) {
            self.bytes -= old;
            self.order.retain(|x| x != &k);
        }
        while self.bytes + size > self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some((_, s)) = self.map.remove(&oldest) {
                self.bytes -= s;
            }
        }
        self.bytes += size;
        self.order.push_back(k.clone());
        self.map.insert(k, (v, size));
    }

    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_cache_evicts_by_bytes() {
        let mut c: BoundedCache<u32, u32> = BoundedCache::new(10);
        c.insert(1, 1, 4);
        c.insert(2, 2, 4);
        c.insert(3, 3, 4);
        assert_eq!(c.get(&1), None);
        assert_eq!(c.get(&3), Some(3));
        assert!(c.bytes() <= 10);
        c.insert(4, 4, 11);
        assert_eq!(c.get(&4), None);
    }
}
