//! Measured costs (ADR 027): what a table-backed step costs is measured,
//! never computed exactly, and every measured number says so.
//!
//! Two models, named exactly as records and results name them:
//! [`MEASURED_PLAN`] for costs from statistics the engine reports
//! (`why` and replay over a table, `EXPLAIN ANALYZE`), and
//! [`MEASURED_WALL`] for wall-clock timing.
//!
//! [`MeasuredScorer`] scores a rewrite: the query before it and the query
//! after it, each measured on the engine, with the noise stated as a bound.
//! It has the shape of evolve's `Scorer` (docs/evolve.md §10: "replay; or
//! an external engine's measured cost"), which has not landed; evolve is
//! meant to take it beside exact replay. Nothing in the engine calls it
//! yet: it runs SQL text, and only text the caller trusts may be given to
//! it (a Proposal's text is an agent's, so the facade, the CLI, and the
//! MCP server never pass one). Each query runs inside a transaction that
//! is rolled back, read-only where the engine has read-only transactions
//! (Postgres) or connections (SQLite).

use std::time::Instant;

use arrow::datatypes::{Schema, SchemaRef};
use bellwether_types::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::driver::{self, Driver};
use crate::url::{EngineKind, SqlUrl};

/// The model of costs read from statistics the engine reports.
pub const MEASURED_PLAN: &str = "measured: engine plan statistics";
/// The model of costs timed on the wall clock.
pub const MEASURED_WALL: &str = "measured: wall time";

/// One side of a rewrite, measured `samples.len()` times.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Measurement {
    /// What was measured: `nanoseconds` (wall time), or the engine's
    /// counter (`blocks` for Postgres buffers, `rows scanned` for DuckDB).
    pub unit: String,
    /// Every sample, in the order taken.
    pub samples: Vec<u64>,
    /// The median sample (the lower middle for an even count).
    pub median: u64,
    /// `max - min` over the samples: the observed spread.
    pub spread: u64,
}

impl Measurement {
    fn of(unit: &str, samples: Vec<u64>) -> Result<Self> {
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        let (Some(&lo), Some(&hi)) = (sorted.first(), sorted.last()) else {
            return Err(Error::Other(
                "a measurement needs at least one sample".into(),
            ));
        };
        Ok(Self {
            unit: unit.to_string(),
            median: sorted[(sorted.len() - 1) / 2],
            spread: hi - lo,
            samples,
        })
    }
}

/// A rewrite's measured cost. Never exact: `exact` is always `false`, and
/// `model` names how it was measured.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasuredScore {
    /// [`MEASURED_PLAN`] or [`MEASURED_WALL`].
    pub model: String,
    /// Always `false` (ADR 027): a measured cost is never stored as exact.
    pub exact: bool,
    /// The query before the rewrite.
    pub before: Measurement,
    /// The query after it.
    pub after: Measurement,
    /// The noise bound: `before.spread + after.spread`. A difference of the
    /// medians within it is not distinguishable from noise.
    pub noise: u64,
    /// Whether `after` is cheaper than `before` by more than `noise`.
    pub improves: bool,
}

impl MeasuredScore {
    fn new(model: &str, before: Measurement, after: Measurement) -> Self {
        let noise = before.spread.saturating_add(after.spread);
        let improves = after.median.saturating_add(noise) < before.median;
        Self {
            model: model.to_string(),
            exact: false,
            before,
            after,
            noise,
            improves,
        }
    }
}

/// Scores a rewrite by measuring it on an engine. The `Scorer` shape of
/// docs/evolve.md §10, for an external engine's measured cost.
pub trait MeasuredScorer {
    /// Measures `before` and `after`, two single SQL queries the caller
    /// trusts, and returns their measured costs with the noise bound.
    fn score(&mut self, before: &str, after: &str) -> Result<MeasuredScore>;
}

fn single_statement(sql: &str) -> Result<&str> {
    let s = sql.trim().trim_end_matches(';').trim();
    if s.is_empty() || s.contains(';') {
        return Err(Error::InvalidArgument {
            field: "rewrite",
            reason: "a measured query is one SQL statement with no `;` inside".into(),
        });
    }
    Ok(s)
}

/// One measured run of `sql` inside a rolled-back transaction.
fn guarded<T>(
    engine: EngineKind,
    driver: &mut dyn Driver,
    run: impl FnOnce(&mut dyn Driver) -> Result<T>,
) -> Result<T> {
    let (begin, end) = match engine {
        EngineKind::Postgres => ("BEGIN READ ONLY", "ROLLBACK"),
        EngineKind::Mssql => ("BEGIN TRANSACTION", "ROLLBACK TRANSACTION"),
        EngineKind::FlightSql => ("", ""),
        EngineKind::Sqlite | EngineKind::Duckdb => ("BEGIN", "ROLLBACK"),
    };
    if !begin.is_empty() {
        driver.execute(begin)?;
    }
    let out = run(driver);
    if !end.is_empty() {
        let rolled = driver.execute(end);
        if out.is_ok() {
            rolled?;
        }
    }
    out
}

/// Wall time: each query runs `repeats` times, alternating before and
/// after so drift in the machine's load falls on both; every result row is
/// fetched and dropped.
pub struct TimingScorer {
    engine: EngineKind,
    driver: Box<dyn Driver>,
    repeats: u32,
}

impl TimingScorer {
    /// Connects to `url` (a `sql://` root; its tables are not used).
    pub fn connect(url: &str, repeats: u32) -> Result<Self> {
        let parsed = SqlUrl::parse(url)?;
        Ok(Self::with_driver(
            parsed.engine,
            driver::open(&parsed, false)?,
            repeats,
        ))
    }

    /// Over a driver the caller opened.
    #[must_use]
    pub fn with_driver(engine: EngineKind, driver: Box<dyn Driver>, repeats: u32) -> Self {
        Self {
            engine,
            driver,
            repeats: repeats.max(1),
        }
    }

    fn once(&mut self, sql: &str) -> Result<u64> {
        let any: SchemaRef = std::sync::Arc::new(Schema::empty());
        guarded(self.engine, &mut *self.driver, |d| {
            let start = Instant::now();
            d.query(sql, &any, &mut |_| Ok(()))?;
            Ok(u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX))
        })
    }
}

impl MeasuredScorer for TimingScorer {
    fn score(&mut self, before: &str, after: &str) -> Result<MeasuredScore> {
        let (b, a) = (single_statement(before)?, single_statement(after)?);
        let (mut bs, mut as_) = (Vec::new(), Vec::new());
        for _ in 0..self.repeats {
            bs.push(self.once(b)?);
            as_.push(self.once(a)?);
        }
        Ok(MeasuredScore::new(
            MEASURED_WALL,
            Measurement::of("nanoseconds", bs)?,
            Measurement::of("nanoseconds", as_)?,
        ))
    }
}

/// Engine plan statistics from `EXPLAIN ANALYZE`: Postgres's shared
/// buffers touched (hit plus read, in blocks), or DuckDB's rows scanned.
/// Other engines are refused (use [`TimingScorer`]).
pub struct ExplainScorer {
    engine: EngineKind,
    driver: Box<dyn Driver>,
    repeats: u32,
}

impl ExplainScorer {
    /// Connects to `url` (a `sql://` root; its tables are not used).
    pub fn connect(url: &str, repeats: u32) -> Result<Self> {
        let parsed = SqlUrl::parse(url)?;
        Self::with_driver(parsed.engine, driver::open(&parsed, false)?, repeats)
    }

    /// Over a driver the caller opened.
    pub fn with_driver(engine: EngineKind, driver: Box<dyn Driver>, repeats: u32) -> Result<Self> {
        if !matches!(engine, EngineKind::Postgres | EngineKind::Duckdb) {
            return Err(Error::Source(format!(
                "{engine} reports no plan statistics this scorer reads; measure wall time \
                 (TimingScorer) instead"
            )));
        }
        Ok(Self {
            engine,
            driver,
            repeats: repeats.max(1),
        })
    }

    fn unit(&self) -> &'static str {
        match self.engine {
            EngineKind::Postgres => "blocks",
            _ => "rows scanned",
        }
    }

    fn once(&mut self, sql: &str) -> Result<u64> {
        let explain = match self.engine {
            EngineKind::Postgres => format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"),
            _ => format!("EXPLAIN (ANALYZE, FORMAT JSON) {sql}"),
        };
        let engine = self.engine;
        let any: SchemaRef = std::sync::Arc::new(Schema::empty());
        let mut text = String::new();
        guarded(engine, &mut *self.driver, |d| {
            d.query(&explain, &any, &mut |b| {
                for col in b.columns() {
                    let s = arrow::compute::cast(col, &arrow::datatypes::DataType::Utf8)
                        .map_err(|e| Error::Source(e.to_string()))?;
                    let s = s
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| Error::Source("EXPLAIN returned no text".into()))?;
                    for v in s.iter().flatten() {
                        text.push_str(v);
                        text.push('\n');
                    }
                }
                Ok(())
            })
        })?;
        plan_counter(engine, &text)
    }
}

/// Sums `keys` over a JSON plan. Postgres reports buffers per node
/// including its children, so with `top_only` the first node that has them
/// is the plan's total.
fn sum(v: &serde_json::Value, keys: &[&str], top_only: bool, depth: usize) -> u64 {
    match v {
        serde_json::Value::Object(m) => {
            let here: u64 = keys
                .iter()
                .filter_map(|k| m.get(*k).and_then(serde_json::Value::as_u64))
                .sum();
            if top_only && here > 0 && depth > 0 {
                return here;
            }
            here + m
                .values()
                .map(|c| sum(c, keys, top_only, depth + 1))
                .sum::<u64>()
        }
        serde_json::Value::Array(a) => a.iter().map(|c| sum(c, keys, top_only, depth + 1)).sum(),
        _ => 0,
    }
}

/// The counter in an `EXPLAIN ANALYZE` JSON answer.
fn plan_counter(engine: EngineKind, text: &str) -> Result<u64> {
    let keys: &[&str] = match engine {
        EngineKind::Postgres => &["Shared Hit Blocks", "Shared Read Blocks"],
        _ => &["rows_scanned", "operator_rows_scanned"],
    };
    let start = text
        .find(['[', '{'])
        .ok_or_else(|| Error::Source(format!("{engine}: EXPLAIN ANALYZE returned no JSON plan")))?;
    let json: serde_json::Value = serde_json::from_str(text[start..].trim()).map_err(|e| {
        Error::Source(format!(
            "{engine}: the EXPLAIN ANALYZE plan is not JSON ({e})"
        ))
    })?;
    // DuckDB 1.2 and later state the plan's total; earlier ones report
    // rows scanned per operator, which are summed.
    if let Some(total) = json
        .get("total_rows_scanned")
        .and_then(serde_json::Value::as_u64)
    {
        return Ok(total);
    }
    Ok(sum(&json, keys, engine == EngineKind::Postgres, 0))
}

impl MeasuredScorer for ExplainScorer {
    fn score(&mut self, before: &str, after: &str) -> Result<MeasuredScore> {
        let (b, a) = (single_statement(before)?, single_statement(after)?);
        let (mut bs, mut as_) = (Vec::new(), Vec::new());
        for _ in 0..self.repeats {
            bs.push(self.once(b)?);
            as_.push(self.once(a)?);
        }
        let unit = self.unit();
        Ok(MeasuredScore::new(
            MEASURED_PLAN,
            Measurement::of(unit, bs)?,
            Measurement::of(unit, as_)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_measured_score_is_never_exact_and_states_its_noise() {
        let s = MeasuredScore::new(
            MEASURED_WALL,
            Measurement::of("nanoseconds", vec![100, 120, 110]).unwrap(),
            Measurement::of("nanoseconds", vec![50, 60, 55]).unwrap(),
        );
        assert!(!s.exact);
        assert_eq!(s.noise, 30);
        assert_eq!(s.before.median, 110);
        assert!(s.improves);
        let close = MeasuredScore::new(
            MEASURED_WALL,
            Measurement::of("nanoseconds", vec![100, 140]).unwrap(),
            Measurement::of("nanoseconds", vec![90, 95]).unwrap(),
        );
        assert!(!close.improves, "within the noise bound");
    }

    #[test]
    fn only_one_statement_is_measured() {
        assert!(single_statement("SELECT 1; DROP TABLE t").is_err());
        assert_eq!(single_statement(" SELECT 1; ").unwrap(), "SELECT 1");
    }

    #[test]
    fn postgres_buffers_are_read_from_the_root_node() {
        let pg = r#"[{"Plan": {"Node Type": "Seq Scan", "Shared Hit Blocks": 3, "Shared Read Blocks": 2,
            "Plans": [{"Shared Hit Blocks": 3, "Shared Read Blocks": 2}]}}]"#;
        assert_eq!(plan_counter(EngineKind::Postgres, pg).unwrap(), 5);
        let duck = r#"{"children": [{"operator_rows_scanned": 10, "children": [{"operator_rows_scanned": 5}]}]}"#;
        assert_eq!(plan_counter(EngineKind::Duckdb, duck).unwrap(), 15);
        let duck2 = r#"{"total_rows_scanned": 7, "operator": [{"rows_scanned": 7}]}"#;
        assert_eq!(plan_counter(EngineKind::Duckdb, duck2).unwrap(), 7);
        assert!(plan_counter(EngineKind::Duckdb, "no plan").is_err());
    }
}
