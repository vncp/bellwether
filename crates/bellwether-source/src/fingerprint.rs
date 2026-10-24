//! File fingerprints per `docs/format.md`:
//! `(path, size, etag_or_mtime, blake3(content))`, where the hashed content
//! is chosen by [`FingerprintPolicy`] (default: the last 64 KiB).
//!
//! Path alone is not identity (ADR 013). An in-place rewrite that changes the
//! size, the etag or mtime, or the hashed bytes is reported by [`drift`].
//!
//! The inherent limits of ADR 013 apply:
//! - **Tail coverage.** Under [`FingerprintPolicy::Tail64K`], an in-place
//!   patch outside the last 64 KiB that keeps the size and the mtime/etag is
//!   **not** detected. Use [`FingerprintPolicy::Full`] for small or critical
//!   files.
//! - **Inode, not bytes.** Verifying an opened handle pins the inode (or the
//!   object version), not its bytes. An in-place write to the same inode
//!   while a read is in progress is not detected under either policy.

use bellwether_types::{Error, FileFingerprint, Hash, Result};
use parquet::file::reader::ChunkReader;

use crate::storage::{LocalFs, Storage};

/// Number of trailing bytes hashed under [`FingerprintPolicy::Tail64K`].
pub const TAIL_BYTES: u64 = 64 * 1024;

/// Window used to stream a file through BLAKE3 under
/// [`FingerprintPolicy::Full`]; bounds memory regardless of file size.
const FULL_HASH_WINDOW: u64 = 1024 * 1024;

/// Which bytes a fingerprint hashes. This corresponds to the
/// `fingerprint_policy` in `bellwether.toml`.
///
/// A fingerprint and every verification against it must use the same policy.
/// Fingerprints taken under different policies compare as drift (never as a
/// false match), except for files of at most 64 KiB, where both policies hash
/// the whole file and agree.
///
/// The hash is stored in `FileFingerprint::tail_blake3`. Under `Full` that
/// field carries the BLAKE3 of the whole file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum FingerprintPolicy {
    /// BLAKE3 of the last 64 KiB (the whole file if smaller). Costs one
    /// bounded read per fingerprint and per verification. Misses an in-place
    /// patch outside the tail that keeps the size and the mtime/etag.
    #[default]
    Tail64K,
    /// BLAKE3 of the whole file, streamed in bounded windows. Detects an
    /// in-place patch anywhere, but reads every byte at every fingerprint and
    /// at every verification, including each `read_columns`. Intended for
    /// small or critical files.
    Full,
}

/// Fingerprints a local file under the default policy. Reads only its
/// metadata and its last 64 KiB.
pub fn fingerprint(path: &str) -> Result<FileFingerprint> {
    fingerprint_with(&LocalFs, path)
}

/// Fingerprints a file through any [`Storage`] under the default policy.
pub fn fingerprint_with<S: Storage>(storage: &S, path: &str) -> Result<FileFingerprint> {
    fingerprint_with_policy(storage, path, FingerprintPolicy::default())
}

/// Fingerprints a file through any [`Storage`] under `policy`. Metadata and
/// content hash both come from one opened handle, so they describe the same
/// file version.
pub fn fingerprint_with_policy<S: Storage>(
    storage: &S,
    path: &str,
    policy: FingerprintPolicy,
) -> Result<FileFingerprint> {
    let (reader, meta) = storage.open(path)?;
    let hash = content_hash(&reader, path, meta.size, policy)?;
    Ok(FileFingerprint {
        path: meta.path,
        size: meta.size,
        etag: meta.etag,
        tail_blake3: hash,
    })
}

/// The content hash of the `size`-byte file behind `reader` under `policy`.
pub(crate) fn content_hash<R: ChunkReader>(
    reader: &R,
    path: &str,
    size: u64,
    policy: FingerprintPolicy,
) -> Result<Hash> {
    let read = |start: u64, len: u64| {
        let len =
            usize::try_from(len).map_err(|_| Error::Source(format!("{path}: read too large")))?;
        reader
            .get_bytes(start, len)
            .map_err(|e| Error::Source(format!("{path}: {e}")))
    };
    match policy {
        FingerprintPolicy::Tail64K => {
            let start = size.saturating_sub(TAIL_BYTES);
            Ok(Hash::of(&read(start, size - start)?))
        }
        FingerprintPolicy::Full => {
            let mut hasher = blake3::Hasher::new();
            let mut pos = 0;
            while pos < size {
                let len = FULL_HASH_WINDOW.min(size - pos);
                hasher.update(&read(pos, len)?);
                pos += len;
            }
            Ok(Hash(*hasher.finalize().as_bytes()))
        }
    }
}

/// Whether `new` describes different file contents than `old`.
///
/// True when any component differs: path, size, etag or mtime, or content
/// hash. Callers pair fingerprints by path, and both must be taken under the
/// same [`FingerprintPolicy`]. A differing path is reported as drift because
/// the two fingerprints cannot be shown to name the same bytes. For local
/// files the mtime is part of the fingerprint, so touching a file without
/// changing its bytes also counts as drift. That errs toward re-indexing.
///
/// `false` is only as strong as the policy. Under `Tail64K`, a same-size,
/// same-mtime patch outside the last 64 KiB is not drift (see the module
/// docs).
#[must_use]
pub fn drift(old: &FileFingerprint, new: &FileFingerprint) -> bool {
    old != new
}
