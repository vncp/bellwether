//! `relational`: the two-dataset corpus relational replay (WP15) is priced
//! and checked on, with the consumer plans that read it.
//!
//! Both datasets sit beside each other under `<out>/`:
//!
//! - `<out>/events/part-NNNN.parquet`: `repo_id: Int64` (null on
//!   `null_fraction` of the rows), `type: Utf8` (`push`, `pull_request`,
//!   `issues`, `fork`, `watch`), `created_at: Timestamp(ms)` (non-decreasing,
//!   several events sharing one millisecond), `amount: Decimal128(18, 2)`.
//!   Events arrive in time order, so one repository's events spread over
//!   many row groups, as an event log's do.
//! - `<out>/repos/part-NNNN.parquet`: `repo_id: Int64`, `name: Utf8`,
//!   `created_at: Timestamp(ms)`, `stars: Decimal128(18, 4)`: one row per
//!   repository, in `repo_id` order, plus restated rows (a later version of
//!   the repository, `restated_fraction` of them, some at the same instant
//!   as the version they restate), so per-key counts differ from 1 and a
//!   `latest` over repositories has ties.
//!
//! `workload/keys-<n>.parquet` holds `n` distinct `repo_id`s (a run's keys,
//! drawn from the repositories with events), and `plans/*.json` the consumer
//! plans (`ConsumerPlan`, version 1) over them: `main` is `events` (the
//! data root), `repos` the dataset bound beside it (`--dataset
//! repos=<out>/repos`). Every plan keys its first step by the run's keys
//! (`KeysFrom::Run`).
//!
//! Memory: one row group's rows per dataset, and the repository list.

use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Decimal128Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bellwether_types::{AggregateFn, ConsumerPlan, JoinKind, KeysFrom, Measure, PlanNode};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::stats::chacha;
use crate::{io_err, write_json, BenchError, Result};

/// The corpus parameters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RelationalParams {
    /// RNG seed.
    pub seed: u64,
    /// Repositories (distinct `repo_id`s in `repos`).
    pub repos: u64,
    /// Events.
    pub events: u64,
    /// Rows per row group of `events`.
    pub event_rows_per_row_group: u32,
    /// Row groups per file of `events`.
    pub event_row_groups_per_file: u32,
    /// Rows per row group of `repos`.
    pub repo_rows_per_row_group: u32,
    /// Fraction of repositories with restated rows.
    pub restated_fraction: f64,
    /// Fraction of events whose `repo_id` is null.
    pub null_fraction: f64,
    /// Keys per run, one key file each.
    pub keysets: Vec<u32>,
}

impl RelationalParams {
    /// The small preset: 2,000 repositories, 200,000 events in row groups
    /// of 4,000 (8 per file), repositories in row groups of 500, a fifth of
    /// them restated, 1% null keys, runs of 10, 100, and 1,000 keys.
    #[must_use]
    pub fn small() -> Self {
        Self {
            seed: 15,
            repos: 2_000,
            events: 200_000,
            event_rows_per_row_group: 4_000,
            event_row_groups_per_file: 8,
            repo_rows_per_row_group: 500,
            restated_fraction: 0.2,
            null_fraction: 0.01,
            keysets: vec![10, 100, 1000],
        }
    }

    /// Checks the parameters.
    pub fn validate(&self) -> Result<()> {
        if self.repos == 0 || self.events == 0 {
            return Err(BenchError::Invalid(
                "the corpus needs at least one repository and one event".into(),
            ));
        }
        if self.event_rows_per_row_group == 0
            || self.event_row_groups_per_file == 0
            || self.repo_rows_per_row_group == 0
        {
            return Err(BenchError::Invalid(
                "rows per row group and row groups per file must be positive".into(),
            ));
        }
        for f in [self.restated_fraction, self.null_fraction] {
            if !(0.0..1.0).contains(&f) {
                return Err(BenchError::Invalid(format!(
                    "fraction {f} is outside [0, 1)"
                )));
            }
        }
        if let Some(k) = self.keysets.iter().find(|k| u64::from(**k) > self.repos) {
            return Err(BenchError::Invalid(format!(
                "a run of {k} keys asks for more repositories than the {} there are",
                self.repos
            )));
        }
        Ok(())
    }
}

/// One dataset's shape as written.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSummary {
    /// Files.
    pub files: u64,
    /// Row groups.
    pub row_groups: u64,
    /// Rows.
    pub rows: u64,
}

/// What [`generate_relational`] wrote.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationalSummary {
    /// `events`.
    pub events: DatasetSummary,
    /// `repos`.
    pub repos: DatasetSummary,
    /// Repositories with more than one row in `repos`.
    pub restated: u64,
    /// Events whose `repo_id` is null.
    pub null_events: u64,
    /// The key files, relative to the corpus.
    pub key_files: Vec<String>,
    /// The plan files, relative to the corpus.
    pub plans: Vec<String>,
}

fn props(rows: u32) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(rows as usize))
        .set_created_by("bellwether-bench relational".to_string())
        .build()
}

fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("repo_id", DataType::Int64, true),
        Field::new("type", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("amount", DataType::Decimal128(18, 2), false),
    ]))
}

fn repos_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("repo_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("stars", DataType::Decimal128(18, 4), false),
    ]))
}

/// One row of either dataset.
struct Row {
    repo_id: Option<i64>,
    text: String,
    at: i64,
    value: i128,
}

/// Writes one dataset, a row group at a time, `per_file` row groups per
/// file.
struct DatasetWriter {
    dir: PathBuf,
    schema: SchemaRef,
    props: WriterProperties,
    rows_per_rg: usize,
    per_file: u64,
    pending: Vec<Row>,
    writer: Option<ArrowWriter<BufWriter<File>>>,
    in_file: u64,
    summary: DatasetSummary,
    decimal: (u8, i8),
}

impl DatasetWriter {
    fn new(
        dir: PathBuf,
        schema: SchemaRef,
        rows: u32,
        per_file: u32,
        decimal: (u8, i8),
    ) -> Result<Self> {
        std::fs::create_dir_all(&dir).map_err(io_err(&dir))?;
        Ok(Self {
            dir,
            schema,
            props: props(rows),
            rows_per_rg: rows as usize,
            per_file: u64::from(per_file),
            pending: Vec::new(),
            writer: None,
            in_file: 0,
            summary: DatasetSummary::default(),
            decimal,
        })
    }

    fn push(&mut self, row: Row) -> Result<()> {
        self.pending.push(row);
        if self.pending.len() == self.rows_per_rg {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if self.writer.is_none() {
            let path = self
                .dir
                .join(format!("part-{:04}.parquet", self.summary.files));
            let f = File::create(&path).map_err(io_err(&path))?;
            self.writer = Some(ArrowWriter::try_new(
                BufWriter::new(f),
                Arc::clone(&self.schema),
                Some(self.props.clone()),
            )?);
            self.summary.files += 1;
        }
        let rows = std::mem::take(&mut self.pending);
        let mut ids = Int64Builder::with_capacity(rows.len());
        let mut text = StringBuilder::new();
        let mut at = TimestampMillisecondBuilder::with_capacity(rows.len());
        let mut value = Decimal128Builder::with_capacity(rows.len())
            .with_precision_and_scale(self.decimal.0, self.decimal.1)?;
        for r in &rows {
            ids.append_option(r.repo_id);
            text.append_value(&r.text);
            at.append_value(r.at);
            value.append_value(r.value);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(ids.finish()),
            Arc::new(text.finish()),
            Arc::new(at.finish()),
            Arc::new(value.finish()),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        if let Some(w) = self.writer.as_mut() {
            w.write(&batch)?;
            w.flush()?;
        }
        self.summary.row_groups += 1;
        self.summary.rows += rows.len() as u64;
        self.in_file += 1;
        if self.in_file == self.per_file {
            self.close()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if let Some(w) = self.writer.take() {
            w.close()?;
        }
        self.in_file = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<DatasetSummary> {
        self.flush()?;
        self.close()?;
        Ok(self.summary)
    }
}

const TYPES: [&str; 5] = ["push", "pull_request", "issues", "fork", "watch"];
/// 2026-01-01T00:00:00Z in milliseconds.
const EPOCH_MS: i64 = 1_767_225_600_000;

fn write_keys(path: &Path, keys: &[i64]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(io_err(parent))?;
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "repo_id",
        DataType::Int64,
        false,
    )]));
    let mut b = Int64Builder::with_capacity(keys.len());
    b.append_slice(keys);
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(b.finish()) as ArrayRef])?;
    let f = File::create(path).map_err(io_err(path))?;
    let mut w = ArrowWriter::try_new(BufWriter::new(f), schema, None)?;
    w.write(&batch)?;
    w.close()?;
    Ok(())
}

fn run_keys(columns: &[&str]) -> (Vec<String>, KeysFrom) {
    (
        columns.iter().map(|c| (*c).to_string()).collect(),
        KeysFrom::Run,
    )
}

fn cols(c: &[&str]) -> Vec<String> {
    c.iter().map(|s| (*s).to_string()).collect()
}

/// The plans written beside the corpus, by file name.
#[must_use]
pub fn plans() -> Vec<(&'static str, ConsumerPlan)> {
    let scan = |dataset: &str, c: &[&str]| PlanNode::Scan {
        dataset: dataset.to_string(),
        columns: cols(c),
    };
    let semi = |input: u32| {
        let (columns, keys) = run_keys(&["repo_id"]);
        PlanNode::SemiJoin {
            input,
            columns,
            keys,
        }
    };
    vec![
        (
            "semijoin.json",
            ConsumerPlan {
                version: 1,
                nodes: vec![
                    scan("main", &["repo_id", "type", "created_at", "amount"]),
                    semi(0),
                    PlanNode::Project {
                        input: 1,
                        columns: cols(&["repo_id", "type"]),
                    },
                ],
                root: 2,
            },
        ),
        (
            "join.json",
            ConsumerPlan {
                version: 1,
                nodes: vec![
                    scan("main", &["repo_id", "type"]),
                    semi(0),
                    scan("repos", &["repo_id", "name"]),
                    PlanNode::Join {
                        left: 1,
                        right: 2,
                        on: vec![("repo_id".into(), "repo_id".into())],
                        kind: JoinKind::Inner,
                    },
                    PlanNode::Project {
                        input: 3,
                        columns: cols(&["repo_id", "type", "name"]),
                    },
                ],
                root: 4,
            },
        ),
        (
            "window.json",
            ConsumerPlan {
                version: 1,
                nodes: vec![
                    scan("main", &["repo_id", "created_at", "amount"]),
                    PlanNode::Window {
                        input: 0,
                        partition_by: cols(&["repo_id"]),
                        keys: Some(KeysFrom::Run),
                    },
                    PlanNode::Project {
                        input: 1,
                        columns: cols(&["repo_id", "created_at"]),
                    },
                ],
                root: 2,
            },
        ),
        (
            "aggregate.json",
            ConsumerPlan {
                version: 1,
                nodes: vec![
                    scan("main", &["repo_id", "amount"]),
                    semi(0),
                    PlanNode::Aggregate {
                        input: 1,
                        group_by: cols(&["repo_id"]),
                        measures: vec![
                            Measure {
                                function: AggregateFn::Count,
                                column: None,
                            },
                            Measure {
                                function: AggregateFn::Sum,
                                column: Some("amount".into()),
                            },
                        ],
                    },
                    PlanNode::Exchange {
                        input: 2,
                        by: cols(&["repo_id"]),
                    },
                ],
                root: 3,
            },
        ),
        (
            "latest.json",
            ConsumerPlan {
                version: 1,
                nodes: vec![
                    scan("repos", &["repo_id", "name", "created_at", "stars"]),
                    semi(0),
                    PlanNode::Latest {
                        input: 1,
                        columns: cols(&["repo_id"]),
                        ts: "created_at".into(),
                    },
                ],
                root: 2,
            },
        ),
    ]
}

/// Writes the corpus, its key files, and its plans under `out`, which must
/// not hold a corpus yet.
pub fn generate_relational(out: &Path, p: &RelationalParams) -> Result<RelationalSummary> {
    p.validate()?;
    for d in ["events", "repos", "workload", "plans"] {
        if out.join(d).exists() {
            return Err(BenchError::Invalid(format!(
                "{} already holds {d}; write the corpus to an empty directory",
                out.display()
            )));
        }
    }
    let mut rng = chacha(&[p.seed, 0x5157_0015]);
    let repos = i64::try_from(p.repos).map_err(|_| BenchError::Invalid("too many repos".into()))?;
    // Repositories: one row each in id order, some restated later (a few
    // at the same instant: a tie at the newest version).
    let mut rw = DatasetWriter::new(
        out.join("repos"),
        repos_schema(),
        p.repo_rows_per_row_group,
        4,
        (18, 4),
    )?;
    let mut restated = 0u64;
    for id in 1..=repos {
        let at = EPOCH_MS + id * 1_000;
        rw.push(Row {
            repo_id: Some(id),
            text: format!("repo-{id:05}"),
            at,
            value: i128::from(rng.gen_range(0..1_000_000_i64)),
        })?;
        if rng.gen_bool(p.restated_fraction) {
            restated += 1;
            let versions = rng.gen_range(1..=2);
            for v in 1..=versions {
                let later = if rng.gen_bool(0.25) { at } else { at + v * 500 };
                rw.push(Row {
                    repo_id: Some(id),
                    text: format!("repo-{id:05}-v{v}"),
                    at: later,
                    value: i128::from(rng.gen_range(0..1_000_000_i64)),
                })?;
            }
        }
    }
    let repos_summary = rw.finish()?;
    // Events, in time order; repositories skewed (a few busy ones), some
    // events with no repository.
    let mut ew = DatasetWriter::new(
        out.join("events"),
        events_schema(),
        p.event_rows_per_row_group,
        p.event_row_groups_per_file,
        (18, 2),
    )?;
    let mut at = EPOCH_MS;
    let mut nulls = 0u64;
    let mut with_events = vec![false; usize::try_from(p.repos).unwrap_or(0) + 1];
    for _ in 0..p.events {
        if rng.gen_bool(0.6) {
            at += rng.gen_range(1..50);
        }
        let repo = if rng.gen_bool(p.null_fraction) {
            nulls += 1;
            None
        } else {
            let u: f64 = rng.gen();
            let id = 1 + ((u * u * u) * (repos as f64)) as i64;
            let id = id.min(repos);
            with_events[id as usize] = true;
            Some(id)
        };
        ew.push(Row {
            repo_id: repo,
            text: TYPES[rng.gen_range(0..TYPES.len())].to_string(),
            at,
            value: i128::from(rng.gen_range(-10_000..1_000_000_i64)),
        })?;
    }
    let events_summary = ew.finish()?;
    // Key files: repositories with events, plus one without, drawn
    // deterministically.
    let active: Vec<i64> = (1..=repos).filter(|i| with_events[*i as usize]).collect();
    let mut key_files = Vec::new();
    for (i, n) in p.keysets.iter().enumerate() {
        let mut keys: Vec<i64> = Vec::with_capacity(*n as usize);
        let mut pick = chacha(&[p.seed, 0x4b45_5953, i as u64]);
        while keys.len() < *n as usize && keys.len() < active.len() {
            let k = active[pick.gen_range(0..active.len())];
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
        keys.sort_unstable();
        let rel = format!("workload/keys-{n}.parquet");
        write_keys(&out.join(&rel), &keys)?;
        key_files.push(rel);
    }
    let mut plan_files = Vec::new();
    for (name, plan) in plans() {
        let rel = format!("plans/{name}");
        write_json(&out.join(&rel), &plan)?;
        plan_files.push(rel);
    }
    let summary = RelationalSummary {
        events: events_summary,
        repos: repos_summary,
        restated,
        null_events: nulls,
        key_files,
        plans: plan_files,
    };
    write_json(&out.join("workload").join("relational.json"), &summary)?;
    Ok(summary)
}
