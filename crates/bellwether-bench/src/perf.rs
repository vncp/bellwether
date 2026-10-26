//! Reading planned row groups for the release measurements (WP22,
//! `scripts/perf.sh`).
//!
//! A lookup's end-to-end time is its plan time plus the time to read the row
//! groups it planned. [`read_row_groups`] does the second half the way the
//! harness executes a plan: every column of every named row group, through
//! the byte-counting reader ([`crate::corpus::RowGroupReader`]), row groups
//! read in parallel on the rayon pool, so `bytes_fetched` is measured, not
//! summed from statistics. [`plan_cells`] takes the row groups from a
//! lookup's `--json` output (the plan inside its evidence), and [`all_cells`]
//! names every row group under a folder: the full scan the index is
//! measured against, through the same reader.
//!
//! Nothing here filters rows to the keys: the residual is the consumer's
//! work and is the same for every strategy that reads the same row groups.
//! The footers of the files read are parsed inside the timed region
//! (`footer_ms`), as a reader that starts from a plan must.

use std::path::{Path, PathBuf};
use std::time::Instant;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::corpus::{ReadBytes, RowGroupReader};
use crate::{io_err, BenchError, Result};

/// Row groups to read, by file.
pub type FileCells = Vec<(PathBuf, Vec<usize>)>;

/// What one read of a set of row groups cost.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ReadReport {
    /// Files read.
    pub files: u64,
    /// Row groups read.
    pub row_groups: u64,
    /// Rows decoded.
    pub rows: u64,
    /// Bytes fetched from storage, counted inside the reader (column-chunk
    /// granularity; `docs/benchmark.md`, "metrics as implemented").
    pub bytes_fetched: u64,
    /// Bytes handed to the page decoder.
    pub bytes_consumed: u64,
    /// Wall time parsing the footers of the files read, ms.
    pub footer_ms: f64,
    /// Wall time reading and decoding the row groups, ms.
    pub read_ms: f64,
    /// `footer_ms + read_ms`.
    pub total_ms: f64,
}

fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

/// Reads every column of every named row group through the byte-counting
/// reader and reports what it cost. Files are opened (footers parsed) in
/// parallel, then row groups are read in parallel.
pub fn read_row_groups(cells: &FileCells) -> Result<ReadReport> {
    let start = Instant::now();
    let readers: Vec<RowGroupReader> = cells
        .par_iter()
        .map(|(path, _)| RowGroupReader::open(path))
        .collect::<Result<_>>()?;
    let footer_ms = ms(start);
    let work: Vec<(usize, usize)> = cells
        .iter()
        .enumerate()
        .flat_map(|(fi, (_, rgs))| rgs.iter().map(move |&rg| (fi, rg)))
        .collect();
    let read_start = Instant::now();
    let per_rg: Vec<(u64, ReadBytes)> = work
        .par_iter()
        .map(|&(fi, rg)| -> Result<(u64, ReadBytes)> {
            let (batches, bytes) = readers[fi].read(rg, None)?;
            let rows = batches.iter().map(|b| b.num_rows() as u64).sum();
            Ok((rows, bytes))
        })
        .collect::<Result<_>>()?;
    let read_ms = ms(read_start);
    let mut report = ReadReport {
        files: cells.iter().filter(|(_, rgs)| !rgs.is_empty()).count() as u64,
        row_groups: work.len() as u64,
        footer_ms,
        read_ms,
        total_ms: ms(start),
        ..ReadReport::default()
    };
    for (rows, bytes) in per_rg {
        report.rows += rows;
        report.bytes_fetched += bytes.fetched;
        report.bytes_consumed += bytes.consumed;
    }
    Ok(report)
}

/// The planned row groups of a lookup's `--json` output: the plan in its
/// evidence record (`evidence.record.body.plan`), or a bare plan object
/// (`entries` at the top level). Entry paths are used as written; the CLI
/// writes absolute paths.
pub fn plan_cells(lookup: &serde_json::Value) -> Result<FileCells> {
    let plan = lookup
        .pointer("/evidence/record/body/plan")
        .or_else(|| lookup.get("plan"))
        .unwrap_or(lookup);
    let entries = plan
        .get("entries")
        .and_then(|e| e.as_array())
        .ok_or_else(|| {
            BenchError::Invalid(
                "no plan in this JSON: expected the --json output of locate, latest, or asof \
             (evidence.record.body.plan.entries) or a plan with entries"
                    .into(),
            )
        })?;
    entries
        .iter()
        .map(|e| {
            let path = e
                .pointer("/file/path")
                .and_then(|p| p.as_str())
                .ok_or_else(|| BenchError::Invalid("a plan entry has no file.path".into()))?;
            let rgs = e
                .get("row_groups")
                .and_then(|r| r.as_array())
                .ok_or_else(|| {
                    BenchError::Invalid(format!("the plan entry for {path} has no row_groups"))
                })?
                .iter()
                .map(|v| {
                    v.as_u64()
                        .and_then(|n| usize::try_from(n).ok())
                        .ok_or_else(|| {
                            BenchError::Invalid(format!(
                                "the plan entry for {path} has a row group that is not a \
                                 non-negative integer: {v}"
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((PathBuf::from(path), rgs))
        })
        .collect()
}

/// Every row group of every `.parquet` file under `root`, in path order.
/// Directories whose names start with `.` (an index or cache kept inside
/// the data folder) are skipped.
pub fn all_cells(root: &Path) -> Result<FileCells> {
    let mut files = Vec::new();
    collect_parquet(root, &mut files)?;
    if files.is_empty() {
        return Err(BenchError::Invalid(format!(
            "no .parquet files under {}; pass the data folder",
            root.display()
        )));
    }
    files.sort();
    files
        .into_par_iter()
        .map(|path| {
            let n = RowGroupReader::open(&path)?.num_row_groups();
            Ok((path, (0..n).collect()))
        })
        .collect()
}

fn collect_parquet(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir).map_err(io_err(dir))? {
        let entry = entry.map_err(io_err(dir))?;
        let path = entry.path();
        let hidden = entry.file_name().to_string_lossy().starts_with('.');
        let kind = entry.file_type().map_err(io_err(&path))?;
        if kind.is_dir() {
            if !hidden {
                collect_parquet(&path, out)?;
            }
        } else if !hidden && path.extension().is_some_and(|e| e == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    /// One file of `rgs` row groups of 100 rows each, under `dir/sub`, plus a
    /// file under a hidden directory that a full scan must skip.
    fn corpus(dir: &Path, rgs: usize) -> PathBuf {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let write = |path: &Path| {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(100))
                .build();
            let mut w = ArrowWriter::try_new(
                std::fs::File::create(path).unwrap(),
                schema.clone(),
                Some(props),
            )
            .unwrap();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from_iter_values(
                    0..(rgs as i64 * 100),
                ))],
            )
            .unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        };
        let file = dir.join("sub").join("a.parquet");
        write(&file);
        write(&dir.join(".bellwether").join("hidden.parquet"));
        file
    }

    #[test]
    fn full_scan_reads_every_row_group_and_skips_hidden_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let file = corpus(dir.path(), 3);
        let cells = all_cells(dir.path()).unwrap();
        assert_eq!(cells, vec![(file, vec![0, 1, 2])]);
        let r = read_row_groups(&cells).unwrap();
        assert_eq!((r.files, r.row_groups, r.rows), (1, 3, 300));
        assert!(r.bytes_fetched > 0 && r.bytes_consumed > 0);
    }

    #[test]
    fn plan_reads_only_the_planned_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let file = corpus(dir.path(), 4);
        let all = read_row_groups(&all_cells(dir.path()).unwrap()).unwrap();
        let lookup = serde_json::json!({"evidence": {"record": {"body": {"plan": {
            "entries": [{"file": {"path": file.to_str().unwrap()}, "row_groups": [1, 3], "bytes": 0}]
        }}}}});
        let cells = plan_cells(&lookup).unwrap();
        let r = read_row_groups(&cells).unwrap();
        assert_eq!((r.files, r.row_groups, r.rows), (1, 2, 200));
        // Two of four row groups: some, but not all, of the column-chunk bytes.
        assert!(r.bytes_fetched > 0 && r.bytes_fetched < all.bytes_fetched);
    }

    #[test]
    fn json_without_a_plan_is_refused_with_what_to_pass() {
        let err = plan_cells(&serde_json::json!({"root": "x"})).unwrap_err();
        assert!(err.to_string().contains("--json output of locate"), "{err}");
        let bad = serde_json::json!({"entries": [{"file": {"path": "a"}, "row_groups": [-1]}]});
        assert!(plan_cells(&bad).is_err());
    }
}
