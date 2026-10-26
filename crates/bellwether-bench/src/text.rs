//! A synthetic text corpus for text keys (B5), in either benchmark shape,
//! with planted terms at known selectivities and exact ground truth.
//!
//! - **ledger**: attribute blobs on a versioned ledger (`entity_id`,
//!   `version_ts`, `body`), partitioned by day, searched by substring.
//! - **market**: documents keyed by symbol and date (`symbol`, `ts`,
//!   `body`), partitioned by day, filtered by token before a downstream
//!   pass.
//!
//! Every `body` is 1–4 KB (`min_bytes..=max_bytes`, drawn per row) of
//! words from a fixed pseudo-word vocabulary over the letters `a`–`j`, so
//! no vocabulary word can hold a planted term (each planted term has a
//! letter outside `a`–`j`). Planted terms, each in a known share of rows:
//!
//! | term | query | share of rows | where |
//! |---|---|---|---|
//! | `kowhai` | `has_token` | 1% | every 100th row |
//! | `lorikeet` | `has_token` | 1% (ledger), one symbol's documents on one day (market) | one block |
//! | `quokka` | `has_token` | 0.01% | every 10,000th row |
//! | `xylozw` | `contains` | 1% | inside a longer word, every 100th row |
//! | `vumbrel` | `contains` | as `lorikeet` | inside a longer word, one block |
//! | `zyqvux` | `contains` | 0.01% | inside a longer word, every 10,000th row |
//!
//! Every other planted occurrence alternates its case (`Kowhai`,
//! `KOWHAI`), so a match needs case folding. The ground truth
//! (`workload/text-truth.json`) lists, per query, the rows and the row
//! groups holding a match, computed while writing: a plan must hold every
//! one of those row groups.

use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, RecordBatch, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::market::FileRowGroup;
use crate::stats::{chacha, mix_all};
use crate::{io_err, write_json, BenchError, Result};

/// Microseconds per day.
const DAY_US: i64 = 86_400 * 1_000_000;
/// 2026-01-01T00:00:00Z in microseconds.
const START_US: i64 = 1_767_225_600 * 1_000_000;
/// Letters vocabulary words are made of.
const VOCAB_LETTERS: &[u8] = b"abcdefghij";
/// Vocabulary words.
const VOCAB: usize = 4096;

/// Which benchmark shape the text rides on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TextShape {
    /// `entity_id`, `version_ts`, `body`.
    Ledger,
    /// `symbol`, `ts`, `body`.
    Market,
}

/// Generator parameters; the corpus is a pure function of them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextParams {
    /// The shape.
    pub shape: TextShape,
    /// RNG seed.
    pub seed: u64,
    /// Day partitions.
    pub days: u32,
    /// Files per day.
    pub files_per_day: u32,
    /// Row groups per file.
    pub row_groups_per_file: u32,
    /// Rows per row group.
    pub rows_per_row_group: u32,
    /// Shortest `body`, in bytes.
    pub min_bytes: u32,
    /// Longest `body`, in bytes.
    pub max_bytes: u32,
}

impl TextParams {
    /// The default corpus: 3 days × 2 files × 4 row groups × 500 rows
    /// (12,000 rows, 24 row groups) of 1–4 KB documents.
    #[must_use]
    pub fn small(shape: TextShape) -> Self {
        Self {
            shape,
            seed: 5,
            days: 3,
            files_per_day: 2,
            row_groups_per_file: 4,
            rows_per_row_group: 500,
            min_bytes: 1024,
            max_bytes: 4096,
        }
    }

    /// Rows in the corpus.
    #[must_use]
    pub fn rows(&self) -> u64 {
        u64::from(self.days)
            * u64::from(self.files_per_day)
            * u64::from(self.row_groups_per_file)
            * u64::from(self.rows_per_row_group)
    }

    fn validate(&self) -> Result<()> {
        let bad = |m: &str| Err(BenchError::Invalid(m.to_string()));
        if self.days == 0
            || self.files_per_day == 0
            || self.row_groups_per_file == 0
            || self.rows_per_row_group == 0
        {
            return bad(
                "days, files per day, row groups per file, and rows per row group must be positive",
            );
        }
        if self.min_bytes == 0 || self.min_bytes > self.max_bytes {
            return bad("body sizes must satisfy 0 < min_bytes <= max_bytes");
        }
        if self.rows() < 10_000 {
            return Err(BenchError::Invalid(format!(
                "a text corpus needs at least 10,000 rows so a 0.01% term is in one, and these \
                 parameters give {} (days x files per day x row groups per file x rows per row \
                 group); raise one of them",
                self.rows()
            )));
        }
        Ok(())
    }
}

/// One planted term and the lookup that finds it.
#[derive(Clone, Copy, Debug)]
struct Planted {
    id: &'static str,
    term: &'static str,
    op: &'static str,
    /// One row in `per` holds it.
    per: u64,
    /// Planted in one contiguous block of rows (else evenly spaced).
    block: bool,
    /// Inside a longer word (a substring), else a word of its own.
    inside: bool,
}

const PLANTED: [Planted; 6] = [
    Planted {
        id: "token-1pct",
        term: "kowhai",
        op: "has_token",
        per: 100,
        block: false,
        inside: false,
    },
    Planted {
        id: "token-1pct-block",
        term: "lorikeet",
        op: "has_token",
        per: 100,
        block: true,
        inside: false,
    },
    Planted {
        id: "token-0.01pct",
        term: "quokka",
        op: "has_token",
        per: 10_000,
        block: false,
        inside: false,
    },
    Planted {
        id: "substring-1pct",
        term: "xylozw",
        op: "contains",
        per: 100,
        block: false,
        inside: true,
    },
    Planted {
        id: "substring-1pct-block",
        term: "vumbrel",
        op: "contains",
        per: 100,
        block: true,
        inside: true,
    },
    Planted {
        id: "substring-0.01pct",
        term: "zyqvux",
        op: "contains",
        per: 10_000,
        block: false,
        inside: true,
    },
];

/// One query of the text workload, with its ground truth.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextQueryTruth {
    /// Query id, for example `token-1pct`.
    pub id: String,
    /// `has_token` or `contains`.
    pub op: String,
    /// The argument (lowercase; the corpus holds it in several cases).
    pub value: String,
    /// The share of rows planted, as a fraction: `matching_rows / rows`.
    pub selectivity: f64,
    /// Whether the planted rows are one block: a contiguous run of rows
    /// (ledger), or one symbol's documents on one day (market).
    pub clustered: bool,
    /// Rows holding a match.
    pub matching_rows: u64,
    /// Row groups holding a match.
    pub matching_row_groups: Vec<FileRowGroup>,
}

/// What [`generate_text`] wrote.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TextSummary {
    /// The parameters.
    pub params: TextParams,
    /// Files written.
    pub files: u64,
    /// Row groups written.
    pub row_groups: u64,
    /// Rows written.
    pub rows: u64,
    /// Bytes of `body` text written (uncompressed).
    pub body_bytes: u64,
    /// The queries and their truth (also in `workload/text-truth.json`).
    pub queries: Vec<TextQueryTruth>,
}

/// The Arrow schema of a shape's files.
#[must_use]
pub fn text_schema(shape: TextShape) -> SchemaRef {
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(match shape {
        TextShape::Ledger => vec![
            Field::new("entity_id", DataType::UInt64, false),
            Field::new("version_ts", ts, false),
            Field::new("body", DataType::Utf8, false),
        ],
        TextShape::Market => vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("ts", ts, false),
            Field::new("body", DataType::Utf8, false),
        ],
    }))
}

fn vocabulary(seed: u64) -> Vec<String> {
    let mut rng = chacha(&[seed, 0x76_6f63_6162]);
    (0..VOCAB)
        .map(|_| {
            let len = rng.gen_range(3..=9);
            (0..len)
                .map(|_| char::from(VOCAB_LETTERS[rng.gen_range(0..VOCAB_LETTERS.len())]))
                .collect()
        })
        .collect()
}

/// The symbol of global row `row` (market shape), `1..=50`.
fn symbol_of(seed: u64, row: u64) -> u64 {
    mix_all(&[seed, row, 1]) % 50 + 1
}

/// The symbol a market burst is about.
const BURST_SYMBOL: u64 = 7;

/// Whether global row `row` (on `day`) of `rows` holds planted term `i`.
/// A ledger block is one contiguous run of rows; a market block is a
/// burst: every document about one symbol on the middle day.
fn planted_in(p: &Planted, i: usize, params: &TextParams, day: u32, row: u64) -> bool {
    let seed = params.seed;
    if p.block {
        match params.shape {
            TextShape::Ledger => {
                let rows = params.rows();
                let len = (rows / p.per).max(1);
                let start = rows / 2;
                (start..start + len).contains(&row)
            }
            TextShape::Market => day == params.days / 2 && symbol_of(seed, row) == BURST_SYMBOL,
        }
    } else {
        // Evenly spaced from a seeded offset: exactly one row in `per`.
        mix_all(&[seed, i as u64])
            .wrapping_add(row)
            .is_multiple_of(p.per)
    }
}

fn cased(term: &str, n: u64) -> String {
    match n % 3 {
        0 => term.to_string(),
        1 => term.to_uppercase(),
        _ => {
            let mut c = term.chars();
            c.next()
                .map(|f| f.to_uppercase().chain(c).collect())
                .unwrap_or_default()
        }
    }
}

/// Writes the corpus under `out`: `data/day=YYYY-MM-DD/part-NNNN.parquet`
/// and `workload/text-truth.json`. Refuses a non-empty `out` that holds no
/// earlier text corpus.
pub fn generate_text(out: &Path, params: &TextParams) -> Result<TextSummary> {
    params.validate()?;
    if out.exists() {
        let earlier = out.join("workload").join("text-truth.json").exists();
        let empty = std::fs::read_dir(out)
            .map_err(io_err(out))?
            .next()
            .is_none();
        if !earlier && !empty {
            return Err(BenchError::Invalid(format!(
                "{} is not empty and holds no text corpus; give an empty or new directory",
                out.display()
            )));
        }
        let data = out.join("data");
        if data.exists() {
            std::fs::remove_dir_all(&data).map_err(io_err(&data))?;
        }
    }
    let vocab = vocabulary(params.seed);
    let schema = text_schema(params.shape);
    let mut hits: Vec<(u64, BTreeSet<FileRowGroup>)> = vec![(0, BTreeSet::new()); PLANTED.len()];
    let (mut files, mut row_groups, mut body_bytes) = (0u64, 0u64, 0u64);
    let mut row = 0u64;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(params.rows_per_row_group as usize + 1))
        .set_created_by("bellwether-bench text".to_string())
        .build();
    for day in 0..params.days {
        let date = crate::market::format_rfc3339_us(START_US + i64::from(day) * DAY_US);
        let date = &date[..10];
        for f in 0..params.files_per_day {
            let rel = format!("data/day={date}/part-{f:04}.parquet");
            let path = out.join(&rel);
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir).map_err(io_err(dir))?;
            }
            let file = File::create(&path).map_err(io_err(&path))?;
            let mut w = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props.clone()))?;
            for rg in 0..params.row_groups_per_file {
                let mut rng = chacha(&[params.seed, u64::from(day), u64::from(f), u64::from(rg)]);
                let n = params.rows_per_row_group as usize;
                let mut body = StringBuilder::with_capacity(n, n * params.max_bytes as usize);
                let mut ts = TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC");
                let mut entity = UInt64Builder::with_capacity(n);
                let mut symbol = StringBuilder::with_capacity(n, n * 5);
                let mut text = String::with_capacity(params.max_bytes as usize + 16);
                for i in 0..n as u64 {
                    let target = rng.gen_range(params.min_bytes..=params.max_bytes) as usize;
                    let mut planted: Vec<String> = Vec::new();
                    for (k, p) in PLANTED.iter().enumerate() {
                        if planted_in(p, k, params, day, row) {
                            let t = cased(p.term, row);
                            planted.push(if p.inside {
                                format!(
                                    "{}{t}{}",
                                    vocab[rng.gen_range(0..VOCAB)],
                                    vocab[rng.gen_range(0..VOCAB)]
                                )
                            } else {
                                t
                            });
                            hits[k].0 += 1;
                            hits[k].1.insert(FileRowGroup {
                                file: rel.clone(),
                                row_group: rg,
                            });
                        }
                    }
                    text.clear();
                    let planted_len: usize = planted.iter().map(|p| p.len() + 1).sum();
                    while text.len() + planted_len < target {
                        if !text.is_empty() {
                            text.push(' ');
                        }
                        text.push_str(&vocab[rng.gen_range(0..VOCAB)]);
                    }
                    for p in planted {
                        // Somewhere in the document, at a word boundary.
                        let at = if text.is_empty() {
                            0
                        } else {
                            let mut at = rng.gen_range(0..=text.len());
                            while at < text.len() && !text.is_char_boundary(at) {
                                at += 1;
                            }
                            text[..at].rfind(' ').map_or(0, |s| s + 1)
                        };
                        text.insert_str(at, &format!("{p} "));
                    }
                    body_bytes += text.len() as u64;
                    body.append_value(&text);
                    let t = START_US
                        + i64::from(day) * DAY_US
                        + ((u64::from(f) * u64::from(params.row_groups_per_file) + u64::from(rg))
                            * u64::from(params.rows_per_row_group)
                            + i) as i64
                            * 1_000_000;
                    ts.append_value(t);
                    entity.append_value(mix_all(&[params.seed, row]) % 100_000);
                    symbol.append_value(format!("S{:04}", symbol_of(params.seed, row)));
                    row += 1;
                }
                let key: ArrayRef = match params.shape {
                    TextShape::Ledger => Arc::new(entity.finish()),
                    TextShape::Market => Arc::new(symbol.finish()),
                };
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![key, Arc::new(ts.finish()), Arc::new(body.finish())],
                )?;
                w.write(&batch)?;
                w.flush()?;
                row_groups += 1;
            }
            w.close()?;
            files += 1;
        }
    }
    let queries: Vec<TextQueryTruth> = PLANTED
        .iter()
        .zip(hits)
        .map(|(p, (rows, rgs))| TextQueryTruth {
            id: p.id.to_string(),
            op: p.op.to_string(),
            value: p.term.to_string(),
            selectivity: rows as f64 / row.max(1) as f64,
            clustered: p.block,
            matching_rows: rows,
            matching_row_groups: rgs.into_iter().collect(),
        })
        .collect();
    let summary = TextSummary {
        params: params.clone(),
        files,
        row_groups,
        rows: row,
        body_bytes,
        queries,
    };
    write_json(&out.join("workload").join("text-truth.json"), &summary)?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No vocabulary word holds a planted term, and every planted term has
    /// a letter outside the vocabulary's.
    #[test]
    fn planted_terms_are_not_in_the_vocabulary() {
        let vocab = vocabulary(5);
        for p in PLANTED {
            assert!(
                p.term.bytes().any(|b| !VOCAB_LETTERS.contains(&b)),
                "{}",
                p.term
            );
            assert!(vocab.iter().all(|w| !w.contains(p.term)));
        }
    }

    /// The written truth matches a scan of the bodies, with case folded.
    #[test]
    fn truth_matches_the_written_bodies() {
        use arrow::array::{Array, StringArray};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let dir = tempfile::tempdir().unwrap();
        let params = TextParams {
            min_bytes: 64,
            max_bytes: 128,
            rows_per_row_group: 1250,
            ..TextParams::small(TextShape::Market)
        };
        let s = generate_text(dir.path(), &params).unwrap();
        assert_eq!(s.rows, 30_000);
        for q in &s.queries {
            let mut rows = 0u64;
            let mut rgs = BTreeSet::new();
            for e in walk(&dir.path().join("data")) {
                let rel = e
                    .strip_prefix(dir.path())
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                let b = ParquetRecordBatchReaderBuilder::try_new(File::open(&e).unwrap())
                    .unwrap()
                    .with_batch_size(1250)
                    .build()
                    .unwrap();
                for (rg, batch) in b.enumerate() {
                    let batch = batch.unwrap();
                    let body = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    for i in 0..body.len() {
                        let v = body.value(i).to_lowercase();
                        let hit = match q.op.as_str() {
                            "contains" => v.contains(&q.value),
                            _ => v.split(' ').any(|w| w == q.value),
                        };
                        if hit {
                            rows += 1;
                            rgs.insert(FileRowGroup {
                                file: rel.clone(),
                                row_group: rg as u32,
                            });
                        }
                    }
                }
            }
            assert_eq!(rows, q.matching_rows, "{}", q.id);
            assert!(rows > 0, "{}", q.id);
            assert_eq!(
                rgs.into_iter().collect::<Vec<_>>(),
                q.matching_row_groups,
                "{}",
                q.id
            );
        }
    }

    fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        for e in std::fs::read_dir(dir).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p);
            }
        }
        out.sort();
        out
    }
}
