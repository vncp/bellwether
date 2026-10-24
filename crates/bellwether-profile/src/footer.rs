//! Footer statistics for every file of a dataset, gathered through the
//! [`Source`] and cached as one artifact (`cache/<input-hash>.bin`).
//!
//! Files are ordered by path and row groups by `index_in_file`. That order is
//! the global row-group order every other measurement in this crate uses.
//!
//! Memory: the statistics of every row group are held at once, `O(row groups
//! x columns)`, the same order as the footers themselves and the manifest's
//! row-group space. Key values are never held here; they are streamed one
//! row group at a time by [`crate::fanout`].

use bellwether_types::{
    ColumnStats, Error, FileFingerprint, Result, RowGroupId, RowGroupMeta, Source,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Format tag of the cached payload. Part of the cache key.
pub const FOOTER_CACHE_FORMAT: &str = "bellwether-profile/footer-stats/1";

/// One file and its row groups as the source reports them.
#[derive(Clone, Debug, PartialEq)]
pub struct FileStats {
    /// The file.
    pub fingerprint: FileFingerprint,
    /// Its row groups, by `index_in_file`.
    pub row_groups: Vec<RowGroupMeta>,
}

/// Footer statistics of a whole dataset.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct FooterStats {
    /// Files by path.
    pub files: Vec<FileStats>,
}

impl FooterStats {
    /// Reads the row groups of every file through `source`. Fails if a path
    /// repeats or if the source reports a row group out of order or for
    /// another file: an ambiguity here would silently shift every count.
    pub fn collect(source: &dyn Source, mut fingerprints: Vec<FileFingerprint>) -> Result<Self> {
        fingerprints.sort_by(|a, b| a.path.cmp(&b.path));
        if let Some(w) = fingerprints.windows(2).find(|w| w[0].path == w[1].path) {
            return Err(Error::Source(format!(
                "{}: discovered twice; file identity is ambiguous",
                w[0].path
            )));
        }
        let mut files = Vec::with_capacity(fingerprints.len());
        for fp in fingerprints {
            let row_groups = source.row_groups(&fp)?;
            for (i, rg) in row_groups.iter().enumerate() {
                if rg.file != fp {
                    return Err(Error::Source(format!(
                        "{}: row group {i} reports file {}",
                        fp.path, rg.file.path
                    )));
                }
                if usize::try_from(rg.index_in_file).ok() != Some(i) {
                    return Err(Error::Source(format!(
                        "{}: row group at position {i} reports index_in_file {}",
                        fp.path, rg.index_in_file
                    )));
                }
            }
            files.push(FileStats {
                fingerprint: fp,
                row_groups,
            });
        }
        Ok(Self { files })
    }

    /// Files in the dataset.
    #[must_use]
    pub fn files_total(&self) -> u64 {
        self.files.len() as u64
    }

    /// Row groups in the dataset.
    #[must_use]
    pub fn row_groups_total(&self) -> u64 {
        self.files.iter().map(|f| f.row_groups.len() as u64).sum()
    }

    /// Rows in the dataset, summed from row-group metadata.
    #[must_use]
    pub fn rows_total(&self) -> u64 {
        self.files
            .iter()
            .flat_map(|f| f.row_groups.iter())
            .map(|rg| rg.rows)
            .sum()
    }

    /// Every row group in global order with its file's position.
    pub fn row_groups(&self) -> impl Iterator<Item = (usize, &RowGroupMeta)> + '_ {
        self.files
            .iter()
            .enumerate()
            .flat_map(|(fi, f)| f.row_groups.iter().map(move |rg| (fi, rg)))
    }

    /// Every row group in global order, cloned: the slice the shared
    /// measures of `bellwether_source::stats` read.
    #[must_use]
    pub fn flat(&self) -> Vec<RowGroupMeta> {
        self.row_groups().map(|(_, rg)| rg.clone()).collect()
    }

    /// Compressed bytes per row of `column`, when every row group reports its
    /// column-chunk size.
    #[must_use]
    pub fn bytes_per_row(&self, column: &str) -> Option<f64> {
        let mut bytes = 0u64;
        let mut rows = 0u64;
        for (_, rg) in self.row_groups() {
            bytes += stats_of(rg, column)?.bytes?;
            rows += rg.rows;
        }
        #[allow(clippy::cast_precision_loss)]
        (rows > 0).then(|| bytes as f64 / rows as f64)
    }

    /// Serializes for the cache. Statistics bytes are written as hex.
    pub fn to_bytes(&self) -> Result<Bytes> {
        let files = self
            .files
            .iter()
            .map(|f| {
                let columns: Vec<String> = f
                    .row_groups
                    .first()
                    .map(|rg| rg.stats.iter().map(|s| s.column.clone()).collect())
                    .unwrap_or_default();
                let row_groups = f
                    .row_groups
                    .iter()
                    .map(|rg| {
                        let names: Vec<&str> = rg.stats.iter().map(|s| s.column.as_str()).collect();
                        if names != columns.iter().map(String::as_str).collect::<Vec<_>>() {
                            return Err(Error::Source(format!(
                                "{}: row group {} reports a different column list",
                                f.fingerprint.path, rg.index_in_file
                            )));
                        }
                        Ok(CachedRowGroup {
                            id: rg.id.map(|i| i.0),
                            rows: rg.rows,
                            bytes: rg.bytes,
                            byte_range: rg.byte_range,
                            stats: rg.stats.iter().map(CachedStats::from).collect(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(CachedFile {
                    fingerprint: f.fingerprint.clone(),
                    columns,
                    row_groups,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let payload = CachedFooters {
            format: FOOTER_CACHE_FORMAT.to_string(),
            files,
        };
        Ok(Bytes::from(serde_json::to_vec(&payload)?))
    }

    /// Parses a cached payload.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let payload: CachedFooters = serde_json::from_slice(bytes)?;
        if payload.format != FOOTER_CACHE_FORMAT {
            return Err(Error::Other(format!(
                "footer cache format {:?}, expected {FOOTER_CACHE_FORMAT:?}",
                payload.format
            )));
        }
        let mut files = Vec::with_capacity(payload.files.len());
        for f in payload.files {
            let mut row_groups = Vec::with_capacity(f.row_groups.len());
            for (i, rg) in f.row_groups.into_iter().enumerate() {
                if rg.stats.len() != f.columns.len() {
                    return Err(Error::Other(format!(
                        "footer cache: {} row group {i} has {} statistics for {} columns",
                        f.fingerprint.path,
                        rg.stats.len(),
                        f.columns.len()
                    )));
                }
                let stats = rg
                    .stats
                    .into_iter()
                    .zip(&f.columns)
                    .map(|(s, c)| s.into_stats(c))
                    .collect::<Result<Vec<_>>>()?;
                row_groups.push(RowGroupMeta {
                    id: rg.id.map(RowGroupId),
                    file: f.fingerprint.clone(),
                    index_in_file: u32::try_from(i)
                        .map_err(|_| Error::Other("footer cache: too many row groups".into()))?,
                    rows: rg.rows,
                    bytes: rg.bytes,
                    stats,
                    byte_range: rg.byte_range,
                });
            }
            files.push(FileStats {
                fingerprint: f.fingerprint,
                row_groups,
            });
        }
        Ok(Self { files })
    }
}

/// The statistics of `column` in `rg`, or `None` if the source reported none.
#[must_use]
pub fn stats_of<'a>(rg: &'a RowGroupMeta, column: &str) -> Option<&'a ColumnStats> {
    rg.stats.iter().find(|s| s.column == column)
}

/// Whether `rg` provably holds no non-null value of `column` (its null count
/// equals its row count).
#[must_use]
pub fn all_null(rg: &RowGroupMeta, column: &str) -> bool {
    rg.rows == 0 || stats_of(rg, column).and_then(|s| s.null_count) == Some(rg.rows)
}

#[derive(Serialize, Deserialize)]
struct CachedFooters {
    format: String,
    files: Vec<CachedFile>,
}

#[derive(Serialize, Deserialize)]
struct CachedFile {
    fingerprint: FileFingerprint,
    columns: Vec<String>,
    row_groups: Vec<CachedRowGroup>,
}

#[derive(Serialize, Deserialize)]
struct CachedRowGroup {
    id: Option<u32>,
    rows: u64,
    bytes: u64,
    byte_range: Option<(u64, u64)>,
    stats: Vec<CachedStats>,
}

#[derive(Serialize, Deserialize)]
struct CachedStats {
    min: Option<String>,
    max: Option<String>,
    nulls: Option<u64>,
    distinct: Option<u64>,
    bloom: bool,
    bytes: Option<u64>,
}

impl From<&ColumnStats> for CachedStats {
    fn from(s: &ColumnStats) -> Self {
        Self {
            min: s.min.as_deref().map(to_hex),
            max: s.max.as_deref().map(to_hex),
            nulls: s.null_count,
            distinct: s.distinct_count,
            bloom: s.has_bloom,
            bytes: s.bytes,
        }
    }
}

impl CachedStats {
    fn into_stats(self, column: &str) -> Result<ColumnStats> {
        Ok(ColumnStats {
            column: column.to_string(),
            min: self.min.as_deref().map(from_hex).transpose()?,
            max: self.max.as_deref().map(from_hex).transpose()?,
            null_count: self.nulls,
            distinct_count: self.distinct,
            has_bloom: self.bloom,
            bytes: self.bytes,
        })
    }
}

fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

fn from_hex(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(Error::Other(format!(
            "footer cache: odd hex length {}",
            s.len()
        )));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|_| Error::Other(format!("footer cache: bad hex {:?}", &s[i..i + 2])))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bellwether_types::Hash;

    fn rg(path: &str, i: u32, min: Option<Vec<u8>>) -> RowGroupMeta {
        RowGroupMeta {
            id: None,
            file: FileFingerprint {
                path: path.into(),
                size: 10,
                etag: Some("e".into()),
                tail_blake3: Hash::of(path.as_bytes()),
            },
            index_in_file: i,
            rows: 5,
            bytes: 100,
            stats: vec![ColumnStats {
                column: "a".into(),
                min: min.clone(),
                max: min,
                null_count: Some(1),
                distinct_count: None,
                has_bloom: true,
                bytes: Some(40),
            }],
            byte_range: Some((0, 10)),
        }
    }

    #[test]
    fn cache_round_trip_is_lossless() {
        let f = rg("x.parquet", 0, None).file;
        let stats = FooterStats {
            files: vec![FileStats {
                fingerprint: f,
                row_groups: vec![
                    rg("x.parquet", 0, Some(vec![0x02, 0x80, 0, 0, 0, 0, 0, 0, 0])),
                    rg("x.parquet", 1, None),
                ],
            }],
        };
        let back = FooterStats::from_bytes(&stats.to_bytes().unwrap()).unwrap();
        assert_eq!(back, stats);
        assert_eq!(back.row_groups_total(), 2);
        assert_eq!(back.rows_total(), 10);
        assert!((back.bytes_per_row("a").unwrap() - 8.0).abs() < 1e-9);
        assert!(back.bytes_per_row("missing").is_none());
    }

    #[test]
    fn foreign_cache_formats_are_refused() {
        let bad = br#"{"format":"other/1","files":[]}"#;
        assert!(FooterStats::from_bytes(bad).is_err());
        assert!(from_hex("abc").is_err());
        assert!(from_hex("zz").is_err());
    }
}
