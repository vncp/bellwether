//! Bytes at the storage boundary: the byte ranges an engine fetched,
//! classified per file against the file's own layout into footer bytes,
//! column-chunk (data) bytes by row group, and everything else (page
//! indexes, bloom filters, gaps a reader coalesced). Engines' own reports
//! are never used; the ranges come from the storage side
//! ([`crate::datafusion::CountingStore`] in process, or an HTTP range
//! server for an engine in another process).

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;

use serde::Serialize;

use crate::{AdapterError, Result};

/// One fetch storage served: `range` of the object at `path`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Fetch {
    /// The object (as the store names it).
    pub path: String,
    /// The bytes served.
    pub range: Range<u64>,
}

/// One Parquet file's layout: its size, footer span, and every column
/// chunk's span by row group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSpans {
    /// The file's size.
    pub size: u64,
    /// The footer's length including the 8-byte trailer.
    pub footer_len: u64,
    /// `(row group, column chunk span)` for every column chunk.
    pub chunks: Vec<(u32, Range<u64>)>,
}

impl FileSpans {
    /// Reads the layout of the local Parquet file at `path` (its trailer
    /// and footer only).
    pub fn of_local(path: &str) -> Result<Self> {
        let bad = |reason: String| AdapterError::Footer {
            path: path.to_string(),
            reason,
        };
        let mut f = std::fs::File::open(path).map_err(|e| bad(e.to_string()))?;
        let size = f.metadata().map_err(|e| bad(e.to_string()))?.len();
        if size < 12 {
            return Err(bad("too short for a Parquet file".into()));
        }
        let mut trailer = [0u8; 8];
        f.seek(SeekFrom::Start(size - 8))
            .and_then(|_| f.read_exact(&mut trailer))
            .map_err(|e| bad(e.to_string()))?;
        if &trailer[4..] != b"PAR1" {
            return Err(bad("no PAR1 trailer".into()));
        }
        let len = u64::from(u32::from_le_bytes([
            trailer[0], trailer[1], trailer[2], trailer[3],
        ]));
        if len + 12 > size {
            return Err(bad("the footer length exceeds the file".into()));
        }
        let mut meta = vec![0u8; usize::try_from(len).map_err(|e| bad(e.to_string()))?];
        f.seek(SeekFrom::Start(size - 8 - len))
            .and_then(|_| f.read_exact(&mut meta))
            .map_err(|e| bad(e.to_string()))?;
        let md = parquet::file::metadata::ParquetMetaDataReader::decode_metadata(&meta)
            .map_err(|e| bad(e.to_string()))?;
        let mut chunks = Vec::new();
        for (i, rg) in md.row_groups().iter().enumerate() {
            let i = u32::try_from(i).map_err(|e| bad(e.to_string()))?;
            for c in rg.columns() {
                let (start, n) = c.byte_range();
                chunks.push((i, start..start + n));
            }
        }
        Ok(Self {
            size,
            footer_len: len + 8,
            chunks,
        })
    }

    /// The bytes of row group `rg`'s column chunks.
    #[must_use]
    pub fn row_group_bytes(&self, rg: u32) -> u64 {
        self.chunks
            .iter()
            .filter(|(r, _)| *r == rg)
            .map(|(_, s)| s.end - s.start)
            .sum()
    }
}

/// What storage served for one file.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct FileBytes {
    /// Fetches.
    pub fetches: u64,
    /// Bytes served, all fetches summed (a byte fetched twice counts
    /// twice).
    pub fetched: u64,
    /// Of those, bytes inside the footer span.
    pub footer: u64,
    /// Of those, bytes inside a column chunk.
    pub data: u64,
    /// Of those, bytes elsewhere (page indexes, bloom filters, coalesced
    /// gaps).
    pub other: u64,
    /// Data bytes per row group.
    pub data_by_row_group: BTreeMap<u32, u64>,
    /// Row groups a fetch touched (any of its column chunk bytes).
    pub row_groups_touched: BTreeSet<u32>,
}

/// What storage served for a run, per file.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Realized {
    /// Per file, by path.
    pub files: BTreeMap<String, FileBytes>,
    /// Fetches of objects with no layout given (a keys file, for example).
    pub unclassified: u64,
}

impl Realized {
    /// Data bytes over every file.
    #[must_use]
    pub fn data(&self) -> u64 {
        self.files.values().map(|f| f.data).sum()
    }

    /// Footer bytes over every file.
    #[must_use]
    pub fn footer(&self) -> u64 {
        self.files.values().map(|f| f.footer).sum()
    }

    /// Other bytes over every file.
    #[must_use]
    pub fn other(&self) -> u64 {
        self.files.values().map(|f| f.other).sum()
    }

    /// Every `(path, row group)` a fetch touched.
    #[must_use]
    pub fn touched(&self) -> BTreeSet<(String, u32)> {
        self.files
            .iter()
            .flat_map(|(p, f)| f.row_groups_touched.iter().map(move |rg| (p.clone(), *rg)))
            .collect()
    }
}

fn overlap(a: &Range<u64>, b: &Range<u64>) -> u64 {
    a.end.min(b.end).saturating_sub(a.start.max(b.start))
}

/// Classifies `fetches` against each file's layout (`layouts`, by the
/// path the fetches name). Overlapping fetches each count.
#[must_use]
pub fn classify<'a>(
    fetches: impl IntoIterator<Item = &'a Fetch>,
    layouts: &BTreeMap<String, FileSpans>,
) -> Realized {
    let mut out = Realized::default();
    for f in fetches {
        let Some(spans) = layouts.get(&f.path) else {
            out.unclassified += f.range.end.saturating_sub(f.range.start);
            continue;
        };
        let e = out.files.entry(f.path.clone()).or_default();
        let n = f.range.end.saturating_sub(f.range.start);
        e.fetches += 1;
        e.fetched += n;
        let footer = overlap(&f.range, &(spans.size - spans.footer_len..spans.size));
        let mut data = 0;
        for (rg, chunk) in &spans.chunks {
            let o = overlap(&f.range, chunk);
            if o > 0 {
                data += o;
                *e.data_by_row_group.entry(*rg).or_default() += o;
                e.row_groups_touched.insert(*rg);
            }
        }
        e.footer += footer;
        e.data += data;
        e.other += n - footer - data;
    }
    out
}
