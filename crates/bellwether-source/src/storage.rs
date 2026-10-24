//! The byte-access seam under every source.
//!
//! Sources never touch the filesystem directly; they go through [`Storage`],
//! whose `Reader` is a parquet [`ChunkReader`]. The Parquet decoder asks it
//! only for the footer and the projected column chunks of the row group being
//! read.
//!
//! What is physically fetched depends on the reader. [`LocalFs`] hands out a
//! `File`, whose `get_read` wraps a `BufReader` (8 KiB read-ahead). The bytes
//! fetched from the OS can therefore extend up to one buffer past a column
//! chunk, although the decoder consumes only the chunk. An object-store reader
//! must implement `get_read(start)` as bounded ranged reads, **never** as one
//! open-ended GET from `start` to EOF, or a single column read would download
//! the rest of the file.
//!
//! Only [`LocalFs`] is implemented here; an object-store implementation lands
//! with `bellwether-store` and plugs in without changing the sources.

use std::fmt;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use bellwether_types::{Error, Result};
use bytes::Bytes;
use parquet::file::reader::ChunkReader;

/// What a store reports about one object without reading it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Path or URL, as passed to the other methods.
    pub path: String,
    /// Size in bytes.
    pub size: u64,
    /// Etag for object stores. For local files, the mtime as an RFC 3339 UTC
    /// string with nanosecond precision (`2024-01-01T00:00:00.000000000Z`).
    /// `None` if neither is available.
    pub etag: Option<String>,
}

/// Read-only, range-addressed access to files.
pub trait Storage: Send + Sync + fmt::Debug {
    /// Random-access reader handed to the Parquet decoder.
    type Reader: ChunkReader + 'static;

    /// Every file under `root`, recursively, sorted by path. Entries whose
    /// name starts with `.` or `_` (hidden files, `_SUCCESS` markers) are
    /// skipped. A `root` that is itself a file lists just that file.
    fn list(&self, root: &str) -> Result<Vec<String>>;

    /// Size and version token of one file.
    fn head(&self, path: &str) -> Result<ObjectMeta>;

    /// Bytes `range` of one file. A range past the end is truncated.
    fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes>;

    /// A random-access reader over one file, plus that file's metadata taken
    /// **from the opened handle** (or, for object stores, from the response
    /// that pins the version being read). Sources compare this metadata
    /// against a fingerprint, so a rename-replace between the check and the
    /// read cannot go unnoticed.
    fn open(&self, path: &str) -> Result<(Self::Reader, ObjectMeta)>;
}

/// The local filesystem.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalFs;

pub(crate) fn io_err(path: &str, e: impl fmt::Display) -> Error {
    Error::Source(format!("{path}: {e}"))
}

/// Formats a duration since the Unix epoch as RFC 3339 UTC with nanosecond
/// precision, for example `2024-01-01T00:00:00.000000000Z`. Fixed width, so
/// strings compare in time order.
#[must_use]
pub fn rfc3339_utc(since_epoch: Duration) -> String {
    let secs = since_epoch.as_secs();
    let nanos = since_epoch.subsec_nanos();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Civil date from days since 1970-01-01 (H. Hinnant's algorithm); all
    // quantities are non-negative here.
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}T{hh:02}:{mm:02}:{ss:02}.{nanos:09}Z")
}

fn skipped(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_none_or(|n| n.starts_with('.') || n.starts_with('_'))
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    let display = dir.display().to_string();
    let entries = std::fs::read_dir(dir).map_err(|e| io_err(&display, e))?;
    for entry in entries {
        let entry = entry.map_err(|e| io_err(&display, e))?;
        if skipped(&entry.file_name()) {
            continue;
        }
        let ty = entry.file_type().map_err(|e| io_err(&display, e))?;
        let path = entry.path();
        if ty.is_dir() {
            walk(&path, out)?;
        } else if ty.is_file() {
            out.push(path);
        }
    }
    Ok(())
}

impl Storage for LocalFs {
    type Reader = File;

    fn list(&self, root: &str) -> Result<Vec<String>> {
        let root_path = Path::new(root);
        let meta = std::fs::metadata(root_path).map_err(|e| io_err(root, e))?;
        if meta.is_file() {
            return Ok(vec![root.to_string()]);
        }
        let mut found = Vec::new();
        walk(root_path, &mut found)?;
        let mut out = found
            .into_iter()
            .map(|p| {
                p.to_str()
                    .map(str::to_string)
                    .ok_or_else(|| io_err(&p.display().to_string(), "path is not UTF-8"))
            })
            .collect::<Result<Vec<_>>>()?;
        out.sort();
        Ok(out)
    }

    fn head(&self, path: &str) -> Result<ObjectMeta> {
        let meta = std::fs::metadata(path).map_err(|e| io_err(path, e))?;
        Ok(local_meta(path, &meta))
    }

    fn read_range(&self, path: &str, range: Range<u64>) -> Result<Bytes> {
        let mut f = File::open(path).map_err(|e| io_err(path, e))?;
        f.seek(SeekFrom::Start(range.start))
            .map_err(|e| io_err(path, e))?;
        let want = range.end.saturating_sub(range.start);
        let mut buf = Vec::with_capacity(usize::try_from(want).unwrap_or(0));
        f.take(want)
            .read_to_end(&mut buf)
            .map_err(|e| io_err(path, e))?;
        Ok(Bytes::from(buf))
    }

    fn open(&self, path: &str) -> Result<(File, ObjectMeta)> {
        let f = File::open(path).map_err(|e| io_err(path, e))?;
        // Metadata of the open handle, not the path: a file renamed over
        // `path` after this point does not change what we read or report.
        let meta = f.metadata().map_err(|e| io_err(path, e))?;
        Ok((f, local_meta(path, &meta)))
    }
}

fn local_meta(path: &str, meta: &std::fs::Metadata) -> ObjectMeta {
    let etag = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(rfc3339_utc);
    ObjectMeta {
        path: path.to_string(),
        size: meta.len(),
        etag,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_known_instants() {
        let at = |s, n| rfc3339_utc(Duration::new(s, n));
        assert_eq!(at(0, 0), "1970-01-01T00:00:00.000000000Z");
        assert_eq!(at(951_782_400, 5), "2000-02-29T00:00:00.000000005Z");
        assert_eq!(
            at(1_704_067_199, 999_999_999),
            "2023-12-31T23:59:59.999999999Z"
        );
        assert_eq!(at(4_107_542_400, 0), "2100-03-01T00:00:00.000000000Z");
    }
}
