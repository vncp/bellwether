//! `expr`: a corpus with a JSON attribute column on each benchmark shape,
//! equality workloads on a JSON path at three selectivities, and exact
//! ground truth (index kind `expr`, ADR 019).
//!
//! The operator an `expr` key serves is a filter whose left side is a
//! function of a column: here `json_get(<column>,'$.<key>') = <value>`,
//! which no index on the column itself can prune.
//!
//! # Shapes
//!
//! Both shapes write rows in time order (an append log, the way such files
//! arrive), cut into `files` files of `row_groups_per_file` row groups of
//! `rows_per_row_group` rows, under `<out>/data/part-NNNN.parquet` (no
//! partition directories):
//!
//! - **ledger**: `entity_id` (`Int64`), `version_ts` (`Timestamp(us, UTC)`),
//!   `amount` (`Int64`), `attrs` (`Utf8`, a JSON object whose `account`
//!   member is a join key: `{"account":"A-M03-2","channel":"web","ref":81}`).
//! - **market**: `ts` (`Timestamp(us, UTC)`), `price` (`Float64`), `size`
//!   (`Int64`), `payload` (`Utf8`, a vendor JSON object whose `symbol`
//!   member keys the print: `{"venue":"X2","symbol":"S-R0007-1","seq":12}`).
//!   The rows span [`MARKET_DAYS`] days, so `date_trunc(ts,day)` is a key
//!   too.
//!
//! # Values and selectivities
//!
//! Row `i` (counted across the corpus) in global row group `g` takes its
//! key value by position, so each class touches a known share of row
//! groups:
//!
//! - `i % 2 == 0`: a **wide** value (`W0` or `W1`, alternating): every row
//!   group holds both, so an index cannot prune (the negative case).
//! - `i % 4 == 1`: a **medium** value `M<s>-<r>`, `s = g / MEDIUM_SPAN`, `r`
//!   drawn from `0..4`: it lives in one span of [`MEDIUM_SPAN`] row groups.
//! - `i % 4 == 3`: a **rare** value `R<g>-<r>`, `r` drawn from `0..4`: it
//!   lives in one row group.
//!
//! The prefix is `A-` (ledger) or `S-` (market). In [`NULL_PER_MILLE`] rows
//! per thousand the member is absent, so the expression is null there and
//! the row is never indexed (ADR 014). Member order and spacing vary by row,
//! which a JSON path does not see.
//!
//! # Workload and truth
//!
//! `<out>/workload/keys-<query>.parquet` holds one column named by the
//! expression text, the key file `bellwether why --from` and `bellwether
//! locate --keys` read. `<out>/workload/truth.json` lists, per query, the
//! keys and the exact row groups and rows holding one: the minimum any
//! correct plan reads, recorded from the rows as they were written.
//!
//! # Memory (ADR 018)
//!
//! One row group's batch at a time; the truth holds one row-group set per
//! distinct value queried, bounded by the corpus's row groups.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::corpus::{days_from_civil, DAY_US};
use crate::stats::chacha;
use crate::{io_err, write_json, BenchError, Result};

/// Format tag written into `workload/truth.json`.
pub const EXPR_TRUTH_FORMAT: &str = "bellwether-bench-expr-truth/1";
/// Row groups one medium value spans.
pub const MEDIUM_SPAN: u64 = 10;
/// Rows per thousand whose JSON object lacks the keyed member.
pub const NULL_PER_MILLE: u64 = 20;
/// Days the market shape's rows span.
pub const MARKET_DAYS: u64 = 5;
/// First instant of both shapes: 2026-01-01T00:00:00Z.
#[must_use]
pub fn start_us() -> i64 {
    days_from_civil(2026, 1, 1) * DAY_US
}

/// A benchmark shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExprShape {
    /// A versioned ledger with a JSON attribute column holding a join key.
    Ledger,
    /// Market prints with a vendor JSON payload keyed by symbol.
    Market,
}

impl ExprShape {
    /// The shape's name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ledger => "ledger",
            Self::Market => "market",
        }
    }

    /// The JSON column.
    #[must_use]
    pub fn json_column(self) -> &'static str {
        match self {
            Self::Ledger => "attrs",
            Self::Market => "payload",
        }
    }

    /// The JSON member the workload filters on.
    #[must_use]
    pub fn member(self) -> &'static str {
        match self {
            Self::Ledger => "account",
            Self::Market => "symbol",
        }
    }

    /// The expression the workload filters on, in normalized form.
    #[must_use]
    pub fn expression(self) -> String {
        format!("json_get({},'$.{}')", self.json_column(), self.member())
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Ledger => "A-",
            Self::Market => "S-",
        }
    }

    /// The Arrow schema of every data file.
    #[must_use]
    pub fn schema(self) -> SchemaRef {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        Arc::new(Schema::new(match self {
            Self::Ledger => vec![
                Field::new("entity_id", DataType::Int64, false),
                Field::new("version_ts", ts, false),
                Field::new("amount", DataType::Int64, false),
                Field::new("attrs", DataType::Utf8, false),
            ],
            Self::Market => vec![
                Field::new("ts", ts, false),
                Field::new("price", DataType::Float64, false),
                Field::new("size", DataType::Int64, false),
                Field::new("payload", DataType::Utf8, false),
            ],
        }))
    }
}

/// Corpus parameters.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExprParams {
    /// The shape.
    pub shape: ExprShape,
    /// RNG seed.
    pub seed: u64,
    /// Files.
    pub files: u64,
    /// Row groups per file.
    pub row_groups_per_file: u64,
    /// Rows per row group.
    pub rows_per_row_group: u64,
    /// Keys per rare or medium query (a wide query takes both wide values).
    pub keys_per_query: u64,
}

impl ExprParams {
    /// The preset: 10 files × 20 row groups × 1,000 rows, 3 keys per query.
    #[must_use]
    pub fn small(shape: ExprShape) -> Self {
        Self {
            shape,
            seed: 42,
            files: 10,
            row_groups_per_file: 20,
            rows_per_row_group: 1_000,
            keys_per_query: 3,
        }
    }

    /// Refuses parameters the layout cannot honor.
    pub fn validate(&self) -> Result<()> {
        let row_groups = self.files * self.row_groups_per_file;
        if self.files == 0 || self.row_groups_per_file == 0 {
            return Err(BenchError::Invalid(
                "expr corpus needs at least one file and one row group per file".into(),
            ));
        }
        if self.rows_per_row_group < 8 || !self.rows_per_row_group.is_multiple_of(4) {
            return Err(BenchError::Invalid(format!(
                "expr corpus rows per row group must be a multiple of 4 and at least 8 (got {}), \
                 so every row group holds every value class",
                self.rows_per_row_group
            )));
        }
        if row_groups < 2 * MEDIUM_SPAN {
            return Err(BenchError::Invalid(format!(
                "expr corpus needs at least {} row groups (files × row groups per file), so \
                 medium values span less than the corpus; got {row_groups}",
                2 * MEDIUM_SPAN
            )));
        }
        if self.keys_per_query == 0 || self.keys_per_query > row_groups / MEDIUM_SPAN {
            return Err(BenchError::Invalid(format!(
                "expr corpus keys per query must be between 1 and {} (one medium value per \
                 span); got {}",
                row_groups / MEDIUM_SPAN,
                self.keys_per_query
            )));
        }
        Ok(())
    }
}

/// One query's ground truth.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExprQuery {
    /// `rare`, `medium`, `wide`, or (market) `day`.
    pub id: String,
    /// The expression, normalized; also the key file's column name.
    pub expression: String,
    /// The key file, relative to the corpus directory.
    pub keys_file: String,
    /// The keys: strings, or for `day` the instant in microseconds.
    pub keys: Vec<String>,
    /// Row groups holding a key: `(file relative to data/, row group in file)`.
    pub row_groups: Vec<(String, u64)>,
    /// Rows holding a key.
    pub rows: u64,
}

/// `workload/truth.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExprTruth {
    /// [`EXPR_TRUTH_FORMAT`].
    pub format: String,
    /// The parameters.
    pub params: ExprParams,
    /// Queries, most selective first.
    pub queries: Vec<ExprQuery>,
}

/// What the generator wrote.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExprSummary {
    /// The shape.
    pub shape: ExprShape,
    /// Files written.
    pub files: u64,
    /// Row groups written.
    pub row_groups: u64,
    /// Rows written.
    pub rows: u64,
    /// Rows whose JSON object lacks the keyed member.
    pub null_rows: u64,
    /// Compressed bytes of the data files.
    pub bytes: u64,
    /// Queries in the workload.
    pub queries: u64,
}

/// The row groups `(file, row group in file)` and the rows holding a
/// query's key.
type Hits = (BTreeSet<(String, u64)>, u64);

/// The key value class and value of row `i` in global row group `g`.
fn value(shape: ExprShape, i: u64, g: u64, r: u64) -> String {
    let p = shape.prefix();
    match i % 4 {
        0 | 2 => format!("{p}W{}", (i / 2) % 2),
        1 => format!("{p}M{:02}-{r}", g / MEDIUM_SPAN),
        _ => format!("{p}R{g:04}-{r}"),
    }
}

/// The JSON object of one row: the keyed member (when present) among two
/// others, member order and spacing varying with `i`.
fn json(shape: ExprShape, i: u64, key: Option<&str>, other: u64) -> String {
    let (m, a, b) = match shape {
        ExprShape::Ledger => (
            "account",
            format!(
                "\"channel\":\"{}\"",
                ["web", "branch", "api"][(i % 3) as usize]
            ),
            format!("\"ref\":{other}"),
        ),
        ExprShape::Market => (
            "symbol",
            format!("\"venue\":\"X{}\"", i % 4),
            format!("\"seq\":{other}"),
        ),
    };
    let mut parts = vec![a, b];
    if let Some(k) = key {
        parts.insert((i % 3) as usize, format!("\"{m}\":\"{k}\""));
    }
    if i.is_multiple_of(5) {
        format!("{{ {} }}", parts.join(", "))
    } else {
        format!("{{{}}}", parts.join(","))
    }
}

fn props(rows_per_row_group: u64) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_row_count(Some(rows_per_row_group as usize))
        .set_created_by("bellwether-bench expr".to_string())
        .build()
}

/// Writes the corpus, its key files, and its truth under `out`.
pub fn generate_expr(out: &Path, params: &ExprParams) -> Result<ExprSummary> {
    params.validate()?;
    let shape = params.shape;
    let data = out.join("data");
    let workload = out.join("workload");
    for d in [&data, &workload] {
        std::fs::create_dir_all(d).map_err(io_err(d))?;
    }
    let row_groups = params.files * params.row_groups_per_file;
    let rows_total = row_groups * params.rows_per_row_group;
    let step_us = (MARKET_DAYS as i64 * DAY_US) / rows_total as i64;
    // The values each query asks for, chosen before any row is written:
    // rare values from evenly spaced row groups, medium values from evenly
    // spaced spans (each value's `r` is 0, which every class draws).
    let k = params.keys_per_query;
    let p = shape.prefix();
    let rare: Vec<String> = (0..k)
        .map(|j| format!("{p}R{:04}-0", (2 * j + 1) * row_groups / (2 * k)))
        .collect();
    let medium: Vec<String> = (0..k)
        .map(|j| {
            let spans = row_groups / MEDIUM_SPAN;
            format!("{p}M{:02}-0", (2 * j + 1) * spans / (2 * k))
        })
        .collect();
    let wide: Vec<String> = vec![format!("{p}W0"), format!("{p}W1")];
    let day_key = start_us() + 2 * DAY_US;
    let mut hits: BTreeMap<&str, Hits> = BTreeMap::new();
    let mut null_rows = 0u64;
    let mut bytes = 0u64;
    let schema = shape.schema();
    for f in 0..params.files {
        let name = format!("part-{f:04}.parquet");
        let path = data.join(&name);
        let file = File::create(&path).map_err(io_err(&path))?;
        let mut writer = ArrowWriter::try_new(
            file,
            Arc::clone(&schema),
            Some(props(params.rows_per_row_group)),
        )?;
        for rg in 0..params.row_groups_per_file {
            let g = f * params.row_groups_per_file + rg;
            let mut rng = chacha(&[params.seed, shape as u64, g]);
            let n = params.rows_per_row_group as usize;
            let mut ts = TimestampMicrosecondBuilder::with_capacity(n);
            let mut num = Int64Builder::with_capacity(n);
            let mut price = Float64Builder::with_capacity(n);
            let mut text = StringBuilder::with_capacity(n, n * 64);
            for j in 0..params.rows_per_row_group {
                let i = g * params.rows_per_row_group + j;
                let t = start_us() + i as i64 * step_us + rng.gen_range(0..step_us.max(1));
                let v = value(shape, i, g, rng.gen_range(0..4u64));
                let present = rng.gen_range(0..1000u64) >= NULL_PER_MILLE;
                if present {
                    for (q, keys) in [("rare", &rare), ("medium", &medium), ("wide", &wide)] {
                        if keys.contains(&v) {
                            let e = hits.entry(q).or_default();
                            e.0.insert((name.clone(), rg));
                            e.1 += 1;
                        }
                    }
                } else {
                    null_rows += 1;
                }
                if (t - start_us()) / DAY_US == (day_key - start_us()) / DAY_US
                    && shape == ExprShape::Market
                {
                    let e = hits.entry("day").or_default();
                    e.0.insert((name.clone(), rg));
                    e.1 += 1;
                }
                let other = rng.gen_range(0..100_000u64);
                ts.append_value(t);
                text.append_value(json(shape, i, present.then_some(v.as_str()), other));
                match shape {
                    ExprShape::Ledger => {
                        num.append_value(rng.gen_range(0..1_000_000i64));
                    }
                    ExprShape::Market => {
                        num.append_value(rng.gen_range(1..10_000i64));
                        price.append_value(f64::from(rng.gen_range(1_000..50_000i32)) / 100.0);
                    }
                }
            }
            let ts: ArrayRef = Arc::new(ts.finish().with_timezone("UTC"));
            let text: ArrayRef = Arc::new(text.finish());
            let columns: Vec<ArrayRef> = match shape {
                ExprShape::Ledger => {
                    let mut ids = Int64Builder::with_capacity(n);
                    for j in 0..params.rows_per_row_group {
                        ids.append_value(((g * params.rows_per_row_group + j) % 50_000) as i64);
                    }
                    vec![Arc::new(ids.finish()), ts, Arc::new(num.finish()), text]
                }
                ExprShape::Market => {
                    vec![ts, Arc::new(price.finish()), Arc::new(num.finish()), text]
                }
            };
            writer.write(&RecordBatch::try_new(Arc::clone(&schema), columns)?)?;
            writer.flush()?;
        }
        writer.close()?;
        bytes += std::fs::metadata(&path).map_err(io_err(&path))?.len();
    }
    let mut queries = Vec::new();
    let mut plan: Vec<(&str, Vec<String>)> = vec![
        ("rare", rare.clone()),
        ("medium", medium.clone()),
        ("wide", wide.clone()),
    ];
    if shape == ExprShape::Market {
        plan.insert(2, ("day", vec![day_key.to_string()]));
    }
    for (id, keys) in plan {
        let expression = if id == "day" {
            "date_trunc(ts,day)".to_string()
        } else {
            shape.expression()
        };
        let keys_file = format!("workload/keys-{id}.parquet");
        write_keys(&out.join(&keys_file), &expression, id == "day", &keys)?;
        let (rgs, rows) = hits.remove(id).unwrap_or_default();
        queries.push(ExprQuery {
            id: id.to_string(),
            expression,
            keys_file,
            keys,
            row_groups: rgs.into_iter().collect(),
            rows,
        });
    }
    let n_queries = queries.len() as u64;
    write_json(
        &workload.join("truth.json"),
        &ExprTruth {
            format: EXPR_TRUTH_FORMAT.into(),
            params: params.clone(),
            queries,
        },
    )?;
    Ok(ExprSummary {
        shape,
        files: params.files,
        row_groups,
        rows: rows_total,
        null_rows,
        bytes,
        queries: n_queries,
    })
}

/// A key file: one column named `expression`, of strings, or (for a
/// `date_trunc` key) UTC microsecond instants.
fn write_keys(path: &Path, expression: &str, instants: bool, keys: &[String]) -> Result<()> {
    let (field, array): (Field, ArrayRef) =
        if instants {
            let mut b = TimestampMicrosecondBuilder::with_capacity(keys.len());
            for k in keys {
                b.append_value(k.parse::<i64>().map_err(|e| {
                    BenchError::Invalid(format!("key {k:?} is not an instant: {e}"))
                })?);
            }
            (
                Field::new(
                    expression,
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    false,
                ),
                Arc::new(b.finish().with_timezone("UTC")),
            )
        } else {
            let mut b = StringBuilder::with_capacity(keys.len(), 16 * keys.len());
            for k in keys {
                b.append_value(k);
            }
            (
                Field::new(expression, DataType::Utf8, false),
                Arc::new(b.finish()),
            )
        };
    let schema = Arc::new(Schema::new(vec![field]));
    let file = File::create(path).map_err(io_err(path))?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
    writer.write(&RecordBatch::try_new(schema, vec![array])?)?;
    writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_classes_follow_the_row_position() {
        assert_eq!(value(ExprShape::Market, 0, 0, 3), "S-W0");
        assert_eq!(value(ExprShape::Market, 2, 0, 3), "S-W1");
        assert_eq!(value(ExprShape::Ledger, 1, 23, 2), "A-M02-2");
        assert_eq!(value(ExprShape::Ledger, 3, 7, 1), "A-R0007-1");
    }

    #[test]
    fn json_rows_parse_and_hold_the_member_when_present() {
        for i in 0..12 {
            for shape in [ExprShape::Ledger, ExprShape::Market] {
                let with: serde_json::Value =
                    serde_json::from_str(&json(shape, i, Some("V"), 9)).unwrap();
                assert_eq!(with[shape.member()], "V");
                let without: serde_json::Value =
                    serde_json::from_str(&json(shape, i, None, 9)).unwrap();
                assert!(without.get(shape.member()).is_none());
            }
        }
    }

    #[test]
    fn bad_parameters_are_refused_with_the_rule() {
        let mut p = ExprParams::small(ExprShape::Ledger);
        p.rows_per_row_group = 10;
        assert!(p
            .validate()
            .unwrap_err()
            .to_string()
            .contains("multiple of 4"));
        let mut p = ExprParams::small(ExprShape::Ledger);
        p.files = 1;
        p.row_groups_per_file = 5;
        assert!(p
            .validate()
            .unwrap_err()
            .to_string()
            .contains("at least 20"));
    }
}
