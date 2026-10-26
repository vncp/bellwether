//! Query engines that own their storage as Bellwether sources (WP20b, ADR
//! 033).
//!
//! An engine is reached through a [`MeasuredEngine`] adapter of three
//! primitives, and only three:
//!
//! - [`MeasuredEngine::submit`] runs one query under a tag bellwether chose
//!   ([`Tag`]), read-only, and discards its rows;
//! - [`MeasuredEngine::export`] runs one query under a tag, read-only, and
//!   writes that same run's rows as Parquet into a scratch folder: the
//!   rows verified are the rows of the run measured, never of a second
//!   run;
//! - [`MeasuredEngine::stats`] reads one run's statistics from the
//!   engine's **own** query log ([`QueryStats`]): rows and bytes read, peak
//!   memory, CPU, duration, result rows. The entry must be the run's: its
//!   tag, its exact text, and its user, one finished entry only, and not
//!   served from a cache; otherwise [`Error::MeasurementRefused`].
//!
//! Nothing else crosses: no cost is computed here, none is accepted from a
//! caller, and every number in a [`QueryStats`] is one the engine logged
//! for a query bellwether submitted. Costs are `measured`, never `exact`,
//! and the model names the log (`<engine>.query_log`).
//!
//! An adapter is also a [`Source`]: its storage
//! units (ClickHouse partitions, Kusto extents) play files of one row group,
//! and [`identity`] hashes the unit list, so a record can say which state of
//! the engine's tables it saw (`docs/format.md`, "Query engine sources").
//!
//! Adapters: [`clickhouse`] (the reference: `system.query_log`, `FORMAT
//! Parquet`, tested end to end against a server in CI) and [`kusto`]
//! (`.show queries`, `.export to storage`, `.show table extents`; tests skip
//! without a cluster). Another engine is one file implementing the trait;
//! the crate README sketches BigQuery, Snowflake, and Postgres.

#![forbid(unsafe_code)]
// Engine names (ClickHouse, Kusto, BigQuery) read as prose here.
#![allow(clippy::doc_markdown)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod clickhouse;
mod http;
#[cfg(feature = "kusto")]
pub mod kusto;
pub mod uri;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bellwether_source_sql::{MeasuredScore, MeasuredScorer, Measurement};
use bellwether_types::{Error, Hash, QueryStats, Result, Source};
use serde::{Deserialize, Serialize};

pub use uri::{is_engine_uri, redact, EngineKind, EngineUri};

/// The most rows a storage unit may hold for [`Source::read_columns`] to
/// read it (ADR 018): 16 Mi rows, as for SQL units.
pub const DEFAULT_MAX_UNIT_ROWS: u64 = 16 * 1024 * 1024;

/// A query's tag: the id the engine logs it under (ClickHouse `query_id`,
/// Kusto `ClientRequestId`). Bellwether chooses every tag; a caller never
/// supplies one, so a log entry found by tag is one bellwether submitted.
/// ASCII letters, digits, and `-` only, so a tag is safe inside any
/// engine's string literal.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Tag(String);

impl Tag {
    /// A fresh run id: 16 hex digits from the clock, the process, and a
    /// counter, hashed.
    #[must_use]
    pub fn run() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let h = Hash::of(format!("{now}:{}:{n}", std::process::id()).as_bytes());
        h.to_hex()[..16].to_string()
    }

    /// The tag `bw-<run>-<role>`. `role` names the query in the run
    /// (`p0-before`, `p0-after-x`).
    pub fn new(run: &str, role: &str) -> Result<Self> {
        let s = format!("bw-{run}-{role}");
        if s.len() > 96 || !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
            return Err(Error::Other(format!(
                "a query tag holds ASCII letters, digits, and '-' only; {s:?} does not"
            )));
        }
        Ok(Self(s))
    }

    /// The tag as the engine logs it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One run: what [`MeasuredEngine::submit`] and [`MeasuredEngine::export`]
/// return, and what [`MeasuredEngine::stats`] reads the log entry of.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResultHandle {
    /// The tag the query ran under.
    pub tag: Tag,
    /// The query text, exactly as given (surrounding whitespace trimmed).
    pub query: String,
    /// The text the engine received for this run and logs under the tag:
    /// the query itself, or the engine's export statement around it.
    pub logged: String,
    /// What the run exported, for [`MeasuredEngine::export`].
    pub exported: Option<Exported>,
}

/// What [`MeasuredEngine::export`] wrote.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Exported {
    /// The Parquet files written, sorted.
    pub files: Vec<PathBuf>,
    /// Their bytes, summed.
    pub bytes: u64,
    /// The rows the engine reported exporting in the same run's own
    /// response, for an engine whose export is a command its query log
    /// does not count the query's rows for (Kusto `.export`: `NumRecords`);
    /// `None` when the log's result rows are the run's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
}

/// A query engine as a measured source: three primitives (module docs).
/// Implementations are read-only towards the engine: they submit the text
/// they are given under a read-only setting where the engine has one, run
/// their own fixed metadata statements, and create nothing inside it.
///
/// The text is the agent's, so it can still tell its runs apart (by its
/// own query id or the `bw-` prefix of the tag): a run's statistics and
/// its rows therefore always come from the same run, never from two.
pub trait MeasuredEngine: Send + Sync {
    /// The engine root, credentials and parameters removed: what records
    /// name (`clickhouse://host:8123/db/table`).
    fn root(&self) -> &str;

    /// The cost model of every number [`MeasuredEngine::stats`] returns:
    /// `<engine>.query_log`.
    fn model(&self) -> &'static str;

    /// Runs one query under `tag`, read-only, with every cache the engine
    /// lets a request turn off turned off, discarding its rows; fails with
    /// the engine's message if the engine refuses or fails it.
    fn submit(&self, query: &str, tag: &Tag) -> Result<ResultHandle>;

    /// Runs one query under `tag` as [`MeasuredEngine::submit`] does and
    /// writes that run's rows as Parquet into the folder `dir`, which must
    /// not exist yet or be empty: one run, whose log entry
    /// [`MeasuredEngine::stats`] reads. Refuses with
    /// [`Error::ExportDenied`] when the engine or the folder does not allow
    /// it, naming the permission needed.
    fn export(&self, query: &str, tag: &Tag, dir: &Path) -> Result<ResultHandle>;

    /// The statistics the engine's own query log holds for `run`, waiting
    /// (bounded) for the log to record it. Fails when the log has no
    /// finished entry for the tag or the query failed; refuses
    /// ([`Error::MeasurementRefused`]) an entry that is not the run's
    /// (another text or user), more than one finished entry under the tag,
    /// a run served from a cache, and a missing statistic.
    fn stats(&self, run: &ResultHandle) -> Result<QueryStats>;
}

/// An engine adapter: a [`Source`] over its storage units and a
/// [`MeasuredEngine`].
pub trait EngineAdapter: Source + MeasuredEngine {}

impl<T: Source + MeasuredEngine> EngineAdapter for T {}

/// The identity of an engine's tables at one moment: BLAKE3 over the
/// engine root and every unit's path and identity string, in path order
/// (the [`Source`] identity method of ADR 027 applied to an engine).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineIdentity {
    /// The hash.
    pub hash: Hash,
    /// Units listed.
    pub units: u64,
    /// Rows the units hold.
    pub rows: u64,
}

/// Reads `engine`'s identity now: one listing of its storage units.
pub fn identity(engine: &dyn EngineAdapter) -> Result<EngineIdentity> {
    let mut units = engine.discover(engine.root())?;
    units.sort_by(|a, b| a.path.cmp(&b.path));
    let listed: Vec<(&str, Option<&str>)> = units
        .iter()
        .map(|f| (f.path.as_str(), f.etag.as_deref()))
        .collect();
    Ok(EngineIdentity {
        hash: Hash::of_record(&(engine.root(), listed))?,
        units: units.len() as u64,
        rows: units.iter().map(|f| f.size).sum(),
    })
}

/// Opens the adapter `uri` names (`clickhouse://`, `kusto://`). `scratch`
/// is the local folder exports land in, when the engine writes exports to
/// storage it names in the URI (Kusto's `scratch=`).
pub fn connect(uri: &str, scratch: Option<&Path>) -> Result<Box<dyn EngineAdapter>> {
    let parsed = EngineUri::parse(uri)?;
    match parsed.kind {
        EngineKind::ClickHouse => Ok(Box::new(clickhouse::ClickHouse::connect(&parsed)?)),
        #[cfg(feature = "kusto")]
        EngineKind::Kusto => Ok(Box::new(kusto::Kusto::connect(&parsed, scratch)?)),
        #[cfg(not(feature = "kusto"))]
        EngineKind::Kusto => {
            let _ = scratch;
            Err(Error::Source(
                "this build has no Kusto adapter (feature `kusto` of bellwether-source-engine)"
                    .into(),
            ))
        }
    }
}

/// Refuses empty query text, and trims surrounding whitespace. The text is
/// otherwise passed through unchanged: no engine's language is parsed
/// here. One request carries one query (ClickHouse refuses several
/// statements in one HTTP request; a Kusto query request runs one query,
/// `let` statements included, and no management command), and read-only
/// is the engine's own setting on the request.
pub fn check_query(text: &str) -> Result<&str> {
    let t = text.trim();
    if t.is_empty() {
        return Err(Error::InvalidArgument {
            field: "rewrite",
            reason: "a query is empty; each side of a rewrite pair is one query".into(),
        });
    }
    Ok(t)
}

/// `num / den` as a decimal string with three places, rounded half to
/// even, or `None` when `den` is 0 (the Prediction ratio, format.md).
#[must_use]
pub fn ratio(num: u64, den: u64) -> Option<String> {
    if den == 0 {
        return None;
    }
    let scaled = u128::from(num) * 1000;
    let d = u128::from(den);
    let (q, r) = (scaled / d, scaled % d);
    let q = match (2 * r).cmp(&d) {
        std::cmp::Ordering::Greater => q + 1,
        std::cmp::Ordering::Equal if q % 2 == 1 => q + 1,
        _ => q,
    };
    Some(format!("{}.{:03}", q / 1000, q % 1000))
}

/// [`MeasuredScorer`] (ADR 027) over a [`MeasuredEngine`]: each side
/// submitted once under its own tag, its peak memory read from the
/// engine's log. The noise bound is 0 with one sample per side: the score
/// says so by its spread, and `improves` compares the medians.
pub struct EngineScorer<'a> {
    engine: &'a dyn MeasuredEngine,
    run: String,
}

impl<'a> EngineScorer<'a> {
    /// A scorer submitting under a fresh run id.
    #[must_use]
    pub fn new(engine: &'a dyn MeasuredEngine) -> Self {
        Self {
            engine,
            run: Tag::run(),
        }
    }
}

impl MeasuredScorer for EngineScorer<'_> {
    fn score(&mut self, before: &str, after: &str) -> Result<MeasuredScore> {
        let (tb, ta) = (
            Tag::new(&self.run, "before")?,
            Tag::new(&self.run, "after")?,
        );
        let hb = self.engine.submit(check_query(before)?, &tb)?;
        let ha = self.engine.submit(check_query(after)?, &ta)?;
        let (b, a) = (self.engine.stats(&hb)?, self.engine.stats(&ha)?);
        let m = |v: u64| Measurement {
            unit: "bytes of peak memory".into(),
            samples: vec![v],
            median: v,
            spread: 0,
        };
        Ok(MeasuredScore {
            model: self.engine.model().to_string(),
            exact: false,
            before: m(b.peak_memory_bytes),
            after: m(a.peak_memory_bytes),
            noise: 0,
            improves: a.peak_memory_bytes < b.peak_memory_bytes,
        })
    }
}

/// The refusal of a log entry as a run's measurement
/// ([`Error::MeasurementRefused`]); `reason` is one of
/// [`bellwether_types::MEASUREMENT_REFUSALS`].
#[must_use]
pub fn refused(engine: &str, tag: &Tag, reason: &'static str, what: &str) -> Error {
    Error::MeasurementRefused {
        engine: engine.to_string(),
        tag: tag.to_string(),
        reason,
        message: format!(
            "{engine}: the log entry of query {tag} cannot stand as its measurement \
             ({reason}): {what}; nothing was recorded"
        ),
    }
}

/// The Parquet files under `dir`, sorted, and their bytes.
pub(crate) fn listed(dir: &Path) -> Result<Exported> {
    let mut files = Vec::new();
    let mut bytes = 0;
    let read = std::fs::read_dir(dir)
        .map_err(|e| Error::Source(format!("cannot list the export {} ({e})", dir.display())))?;
    for entry in read {
        let entry =
            entry.map_err(|e| Error::Source(format!("cannot list {} ({e})", dir.display())))?;
        let path = entry.path();
        if path.extension().is_some_and(|x| x == "parquet") {
            bytes += entry.metadata().map_or(0, |m| m.len());
            files.push(path);
        }
    }
    files.sort();
    Ok(Exported {
        files,
        bytes,
        rows: None,
    })
}

/// Creates `dir` for an export, or refuses: it exists and is not empty, or
/// it cannot be created (the scratch folder's permission).
pub(crate) fn export_dir(engine: &str, dir: &Path) -> Result<()> {
    if dir.exists() && std::fs::read_dir(dir).map_or(true, |mut d| d.next().is_some()) {
        return Err(Error::Other(format!(
            "the export folder {} exists and is not empty; exports go to a fresh folder",
            dir.display()
        )));
    }
    std::fs::create_dir_all(dir).map_err(|e| denied_scratch(engine, dir, &e.to_string()))
}

/// The export refusal for a scratch folder bellwether cannot write.
pub(crate) fn denied_scratch(engine: &str, dir: &Path, why: &str) -> Error {
    let permission = format!("write access to the scratch folder {}", dir.display());
    Error::ExportDenied {
        message: format!(
            "{engine}: cannot export into {} ({why}); grant {permission} to the account \
             bellwether runs as, or name another scratch folder",
            dir.display()
        ),
        engine: engine.to_string(),
        permission,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ratios_round_half_to_even_and_refuse_a_zero_denominator() {
        assert_eq!(ratio(1, 3).as_deref(), Some("0.333"));
        assert_eq!(ratio(2, 3).as_deref(), Some("0.667"));
        assert_eq!(ratio(1, 2000).as_deref(), Some("0.000"));
        assert_eq!(ratio(3, 2000).as_deref(), Some("0.002"));
        assert_eq!(ratio(5, 1).as_deref(), Some("5.000"));
        assert_eq!(ratio(u64::MAX, 1).unwrap(), format!("{}.000", u64::MAX));
        assert_eq!(ratio(7, 0), None);
    }

    #[test]
    fn tags_are_safe_literals() {
        let t = Tag::new("0123456789abcdef", "p0-before").unwrap();
        assert_eq!(t.as_str(), "bw-0123456789abcdef-p0-before");
        assert!(Tag::new("x", "a'b").is_err());
        assert!(Tag::new("x", "a b").is_err());
        assert_ne!(Tag::run(), Tag::run());
    }

    #[test]
    fn an_empty_query_is_refused_and_text_is_otherwise_unchanged() {
        assert_eq!(check_query("  SELECT 1;\n").unwrap(), "SELECT 1;");
        assert_eq!(
            check_query("let k = 1; T | where x == k").unwrap(),
            "let k = 1; T | where x == k"
        );
        assert!(check_query(" \n\t").is_err());
    }
}
