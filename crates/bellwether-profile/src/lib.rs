//! Profiles a folder of columnar files: which columns discriminate rows and
//! which only look like they do (WP5).
//!
//! [`profile`] measures, for every candidate key column and every candidate
//! composite of them, the **fanout**: how many files and row groups one key
//! value touches, at nearest-rank p50, p95, and max over distinct values,
//! with `files_total` and `row_groups_total` as denominators. It analyzes
//! row-group ranges (typical overlap and sortedness from footers, and for
//! date and timestamp columns the shared overlap depth of
//! `bellwether_source::stats`), runs the statistics-only lints of
//! `docs/rule-catalog.md`, and summarizes a recorded workload. [`evidence()`]
//! turns the report into a `profile` [`Evidence`] whose provenance cites the
//! cache artifacts it was computed from, and [`record`] writes it through the
//! ledger. [`render()`] prints the one-screen summary.
//!
//! - Exactness: while a key's table never fills, its fanout is exact and
//!   `sampled` is false. Past that, quantiles come from a uniform sample of
//!   distinct values (sampling is by value, never by row), each with exact
//!   counts, and `distinct` from a fixed-size HyperLogLog sketch;
//!   [`SampleInfo`] states each key's own error. See [`sampler`] and
//!   [`fanout`].
//! - A composite is measured in the order it is listed (ADR 014 makes the
//!   order matter when a column has nulls): the columns a recorded workload
//!   supplies first, the rest in schema order.
//! - Memory (ADR 018): one row group's key columns at a time, fixed-size
//!   sketches, and sampler tables whose bound [`MemoryReport`] states.
//! - Cache: footer statistics, fanout tables (integers only), and workload
//!   summaries are reused by input hash and their age is reported
//!   ([`cache`]).
//! - Invariant 6: every Evidence carries provenance ([`mod@evidence`]).
//!
//! Rules that need a consumer's query plan (`unprefiltered_window`,
//! `lookback_unbounded`, `wide_projection_before_filter`) are out of scope.

#![forbid(unsafe_code)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
// Proper names (HyperLogLog, BLAKE3, Parquet) read better without backticks.
#![allow(clippy::doc_markdown)]

pub mod cache;
pub mod evidence;
pub mod fanout;
pub mod footer;
pub mod hll;
pub mod lints;
pub mod manifest;
pub mod quantile;
pub mod ranges;
pub mod render;
pub mod sampler;
pub mod workload;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bellwether_ledger::Ledger;
use bellwether_source::stats::overlap_depth;
use bellwether_store::StoreExt;
use bellwether_types::{format, Error, Evidence, FileFingerprint, Hash, Lint, Result, Source};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub use evidence::{evidence, record, InputKind, InputRef};
pub use fanout::{
    CachedEntry, DistinctSource, FanoutEntry, KeyPlan, MemoryReport, SampleInfo, SkippedColumn,
};
pub use footer::FooterStats;
pub use manifest::ManifestInfo;
pub use ranges::{ColumnRanges, TimeScatter};
pub use render::render;
pub use workload::WorkloadSummary;

/// Format tag of the cached fanout table. Part of the cache key; a change to
/// the cached layout must bump it (a unit test pins the layout).
pub const FANOUT_CACHE_FORMAT: &str = "bellwether-profile/fanout/3";
/// Default bytes for all sampler tables together.
pub const DEFAULT_MEMORY_BUDGET: usize = 256 << 20;
/// Default HyperLogLog precision: 16 KiB per key, 0.81 % standard error.
pub const DEFAULT_HLL_PRECISION: u8 = 14;
/// Default seed of the value hash ("bellweth").
pub const DEFAULT_SEED: u64 = 0x6265_6c6c_7765_7468;

/// What to profile and how.
#[derive(Clone, Debug)]
pub struct ProfileOptions {
    /// Candidate key columns. `None` selects every id-like column, skipping
    /// strings and binary wider than [`fanout::AUTO_MAX_BYTES_PER_ROW`]
    /// compressed bytes per row; the report lists what it skipped and why.
    pub columns: Option<Vec<String>>,
    /// Date or timestamp columns for the range analysis. `None` selects all
    /// of them.
    pub time_columns: Option<Vec<String>>,
    /// Largest composite measured (`k`); 1 measures single columns only.
    pub max_tuple_len: usize,
    /// Most composites measured.
    pub max_tuples: usize,
    /// Bytes for all sampler tables together (see [`MemoryReport`]).
    pub memory_budget: usize,
    /// Overrides the per-key table size the budget implies.
    pub max_tracked_per_key: Option<usize>,
    /// HyperLogLog precision, 4..=18.
    pub hll_precision: u8,
    /// Seed of the value hash; fixed so a profile is reproducible.
    pub seed: u64,
    /// A recorded workload directory (bench format), if any.
    pub workload: Option<PathBuf>,
    /// Columns a workload supplies (from a key file or a recorded workload),
    /// when `workload` is not a bench workload directory.
    pub supplied_columns: Vec<String>,
    /// Describes the source's configuration, folded into the footer cache
    /// key. **Required**: an unset value is an error. It must change whenever
    /// the source would report different row groups for the same files, for
    /// example a text source's chunk size (`"csv:chunk=67108864"`); a Parquet
    /// source is `"parquet"`.
    pub source_config: Option<String>,
    /// Reuse cached artifacts. `false` recomputes and overwrites them.
    pub reuse_cache: bool,
    /// Never evict from sampler tables. Breaks the memory bound; exists only
    /// as the negative control of the memory-bound test.
    #[doc(hidden)]
    pub unbounded_tables: bool,
}

impl Default for ProfileOptions {
    fn default() -> Self {
        Self {
            columns: None,
            time_columns: None,
            max_tuple_len: 2,
            max_tuples: 64,
            memory_budget: DEFAULT_MEMORY_BUDGET,
            max_tracked_per_key: None,
            hll_precision: DEFAULT_HLL_PRECISION,
            seed: DEFAULT_SEED,
            workload: None,
            supplied_columns: Vec::new(),
            source_config: None,
            reuse_cache: true,
            unbounded_tables: false,
        }
    }
}

impl ProfileOptions {
    /// The defaults for a source described by `source_config`.
    #[must_use]
    pub fn for_source(source_config: &str) -> Self {
        Self {
            source_config: Some(source_config.to_string()),
            ..Self::default()
        }
    }
}

/// Everything a profile measured.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProfileReport {
    /// Dataset root as given.
    pub root: String,
    /// Files profiled.
    pub files_total: u64,
    /// Row groups profiled.
    pub row_groups_total: u64,
    /// Rows, summed from row-group metadata.
    pub rows_total: u64,
    /// Candidate key columns, schema order.
    pub key_columns: Vec<String>,
    /// Schema columns auto selection skipped, with the reason.
    pub skipped: Vec<SkippedColumn>,
    /// Time columns analyzed.
    pub time_columns: Vec<String>,
    /// Single columns in schema order, then composites ranked by p95 files
    /// per value over `files_total`, then p95 row groups over
    /// `row_groups_total`. Every composite is listed in the order it was
    /// measured: columns the recorded workload supplies first, the rest in
    /// schema order.
    pub fanout: Vec<FanoutEntry>,
    /// Footer range statistics per analyzed column.
    pub ranges: Vec<ColumnRanges>,
    /// The shared overlap depth per time column.
    pub time_scatter: Vec<TimeScatter>,
    /// Lints raised.
    pub lints: Vec<Lint>,
    /// The recorded workload, if one was given.
    pub workload: Option<WorkloadSummary>,
    /// The manifest, if the store holds one.
    pub manifest: Option<ManifestInfo>,
    /// Provenance inputs with their ages.
    pub inputs: Vec<InputRef>,
    /// Memory the fanout pass was allowed and used.
    pub memory: MemoryReport,
}

#[derive(Serialize)]
struct FooterKey<'a> {
    format: &'static str,
    encoding_version: u32,
    source_config: &'a str,
    root: &'a str,
    schema: Vec<(String, String, bool)>,
    files: &'a [FileFingerprint],
}

#[derive(Serialize)]
struct FanoutKey<'a> {
    format: &'static str,
    footers: Hash,
    columns: Vec<(&'a str, String)>,
    keys: &'a [Vec<usize>],
    high_water: u64,
    unbounded: bool,
    hll_precision: u8,
    seed: u64,
    quantile_alpha: f64,
}

/// The cached fanout table: integers only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FanoutTable {
    /// [`FANOUT_CACHE_FORMAT`].
    pub format: String,
    /// One entry per key, in plan order.
    pub entries: Vec<CachedEntry>,
    /// Memory the pass used.
    pub memory: MemoryReport,
}

fn required_source_config(options: &ProfileOptions) -> Result<&str> {
    options.source_config.as_deref().ok_or_else(|| {
        Error::Other(
            "ProfileOptions.source_config is unset: name the source configuration (\"parquet\", \
             or a text source's format and chunk size) so cached footers are never shared across \
             configurations"
                .into(),
        )
    })
}

/// Range statistics of every key column, then of every time column not
/// already among them.
fn analyze_ranges(plan: &KeyPlan, footers: &FooterStats) -> Vec<ColumnRanges> {
    let mut analyzed: Vec<&str> = plan.columns.iter().map(|c| c.name.as_str()).collect();
    for t in &plan.time_columns {
        if !analyzed.contains(&t.name.as_str()) {
            analyzed.push(&t.name);
        }
    }
    analyzed
        .iter()
        .map(|c| ranges::analyze(footers, c))
        .collect()
}

/// Profiles the dataset under `root`.
///
/// `store` is rooted at the index root (`docs/format.md`): the cache lives
/// under `cache/`, the manifest, if any, at `manifest.json`. The folder need
/// not be indexed. `options.source_config` must be set.
pub fn profile<S>(
    source: &dyn Source,
    root: &str,
    store: &Arc<S>,
    options: &ProfileOptions,
) -> Result<ProfileReport>
where
    S: StoreExt + ?Sized,
{
    let source_config = required_source_config(options)?;
    let schema = source.schema(root)?;
    let footers = load_footers(source, root, &schema, store, options, source_config)?;
    let mut inputs = vec![InputRef {
        kind: InputKind::FooterStats,
        hash: footers.key,
        age: footers.age,
        cache_hit: footers.hit,
    }];
    let workload = match &options.workload {
        Some(path) => Some(load_workload(
            store,
            path,
            options.reuse_cache,
            &mut inputs,
        )?),
        None => None,
    };
    let supplied: Vec<String> = workload.as_ref().map_or_else(
        || options.supplied_columns.clone(),
        |w| w.key_columns.clone(),
    );
    let plan = KeyPlan::build(
        &schema,
        &footers.value,
        options.columns.as_deref(),
        options.time_columns.as_deref(),
        options.max_tuple_len,
        options.max_tuples,
        &supplied,
    )?;
    let ranges = analyze_ranges(&plan, &footers.value);
    let table = load_fanout(source, store, options, &footers, &plan, &ranges)?;
    inputs.insert(
        1,
        InputRef {
            kind: InputKind::FanoutTable,
            hash: table.key,
            age: table.age,
            cache_hit: table.hit,
        },
    );
    let manifest = load_manifest(store, root, &mut inputs)?;

    let row_groups = footers.value.flat();
    let time_scatter = plan
        .time_columns
        .iter()
        .map(|t| {
            Ok(TimeScatter::new(
                &t.name,
                &overlap_depth(&row_groups, &t.name)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let entries = table
        .value
        .entries
        .into_iter()
        .map(CachedEntry::into_entry)
        .collect::<Result<Vec<_>>>()?;
    let rows_total = footers.value.rows_total();
    let lints = lints::run(&lints::LintInputs {
        plan: &plan,
        entries: &entries,
        ranges: &ranges,
        time: &time_scatter,
        row_groups: &row_groups,
        workload: workload.as_ref(),
        supplied_columns: &supplied,
        manifest: manifest.as_ref(),
        rows_total,
    })?;
    Ok(ProfileReport {
        root: root.to_string(),
        files_total: footers.value.files_total(),
        row_groups_total: footers.value.row_groups_total(),
        rows_total,
        key_columns: plan.columns.iter().map(|c| c.name.clone()).collect(),
        skipped: plan.skipped.clone(),
        time_columns: plan.time_columns.iter().map(|c| c.name.clone()).collect(),
        fanout: rank(entries),
        ranges,
        time_scatter,
        lints,
        workload,
        manifest,
        inputs,
        memory: table.value.memory,
    })
}

/// Footer statistics of every discovered file, from the cache when their
/// fingerprints, the schema, and the source configuration are unchanged.
fn load_footers<S>(
    source: &dyn Source,
    root: &str,
    schema: &SchemaRef,
    store: &Arc<S>,
    options: &ProfileOptions,
    source_config: &str,
) -> Result<cache::Cached<FooterStats>>
where
    S: StoreExt + ?Sized,
{
    let mut files = source.discover(root)?;
    if files.is_empty() {
        return Err(Error::Source(format!("{root}: no files to profile")));
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let key = cache::key(&FooterKey {
        format: footer::FOOTER_CACHE_FORMAT,
        encoding_version: format::ENCODING_VERSION,
        source_config,
        root,
        schema: schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().to_string(), f.is_nullable()))
            .collect(),
        files: &files,
    })?;
    cache::load_or_compute(
        store,
        key,
        options.reuse_cache,
        FooterStats::from_bytes,
        FooterStats::to_bytes,
        || FooterStats::collect(source, files.clone()),
    )
}

/// The fanout table, from the cache when every input and option that changes
/// a number is unchanged.
fn load_fanout<S>(
    source: &dyn Source,
    store: &Arc<S>,
    options: &ProfileOptions,
    footers: &cache::Cached<FooterStats>,
    plan: &KeyPlan,
    ranges: &[ColumnRanges],
) -> Result<cache::Cached<FanoutTable>>
where
    S: StoreExt + ?Sized,
{
    let config = fanout::PassConfig {
        memory_budget: options.memory_budget,
        max_tracked_per_key: options.max_tracked_per_key,
        hll_precision: options.hll_precision,
        seed: options.seed,
        unbounded: options.unbounded_tables,
    };
    let high_water = fanout::resolve_high_water(&config, plan.keys.len());
    let key = cache::key(&FanoutKey {
        format: FANOUT_CACHE_FORMAT,
        footers: footers.key,
        columns: plan
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.data_type.to_string()))
            .collect(),
        keys: &plan.keys,
        high_water: high_water as u64,
        unbounded: options.unbounded_tables,
        hll_precision: options.hll_precision,
        seed: options.seed,
        quantile_alpha: fanout::QUANTILE_ALPHA,
    })?;
    let n_keys = plan.keys.len();
    cache::load_or_compute(
        store,
        key,
        options.reuse_cache,
        |bytes| {
            let t: FanoutTable = serde_json::from_slice(bytes)?;
            if t.format != FANOUT_CACHE_FORMAT || t.entries.len() != n_keys {
                return Err(Error::Other(
                    "fanout cache entry does not match the plan".into(),
                ));
            }
            Ok(t)
        },
        |t| Ok(Bytes::from(serde_json::to_vec(t)?)),
        || {
            let pass = fanout::key_pass(source, &footers.value, plan, ranges, &config)?;
            Ok(FanoutTable {
                format: FANOUT_CACHE_FORMAT.to_string(),
                entries: pass
                    .entries
                    .iter()
                    .map(CachedEntry::from_entry)
                    .collect::<Result<Vec<_>>>()?,
                memory: pass.memory,
            })
        },
    )
}

/// The verified workload summary, cited twice: the cached summary artifact
/// and the workload's own hash.
fn load_workload<S>(
    store: &Arc<S>,
    path: &Path,
    reuse: bool,
    inputs: &mut Vec<InputRef>,
) -> Result<WorkloadSummary>
where
    S: StoreExt + ?Sized,
{
    let key = cache::key(&workload::cache_key(path)?)?;
    let summary = cache::load_or_compute(
        store,
        key,
        reuse,
        |b| Ok(serde_json::from_slice::<WorkloadSummary>(b)?),
        |s| Ok(Bytes::from(serde_json::to_vec(s)?)),
        || workload::summarize(path),
    )?;
    inputs.push(InputRef {
        kind: InputKind::WorkloadSummary,
        hash: summary.key,
        age: summary.age,
        cache_hit: summary.hit,
    });
    inputs.push(InputRef {
        kind: InputKind::Workload,
        hash: summary.value.hash,
        age: workload::age(path)?,
        cache_hit: false,
    });
    Ok(summary.value)
}

/// The manifest at the store root, if the folder is indexed. A manifest that
/// indexes another root is an error, not a silent mismatch.
fn load_manifest<S>(
    store: &Arc<S>,
    root: &str,
    inputs: &mut Vec<InputRef>,
) -> Result<Option<ManifestInfo>>
where
    S: StoreExt + ?Sized,
{
    let Some((info, age)) = manifest::read(&**store)? else {
        return Ok(None);
    };
    if !manifest::same_root(&info.source_root, root) {
        return Err(Error::Other(format!(
            "the manifest in this store indexes {:?}, not {root:?}",
            info.source_root
        )));
    }
    inputs.push(InputRef {
        kind: InputKind::Manifest,
        hash: info.hash,
        age,
        cache_hit: false,
    });
    Ok(Some(info))
}

/// Profiles, then writes the Evidence through the ledger at the store root.
/// Returns the report, the Evidence, and its hash.
pub fn profile_and_record<S>(
    source: &dyn Source,
    root: &str,
    store: &Arc<S>,
    options: &ProfileOptions,
) -> Result<(ProfileReport, Evidence, Hash)>
where
    S: StoreExt + ?Sized,
{
    let report = profile(source, root, store, options)?;
    let ledger = Ledger::new(Arc::clone(store));
    let (e, hash) = record(&ledger, &report)?;
    Ok((report, e, hash))
}

/// Singles in plan order, then composites by p95 files per value over files
/// total, then p95 row groups over row groups total; ties keep plan order.
fn rank(entries: Vec<FanoutEntry>) -> Vec<FanoutEntry> {
    let (singles, mut tuples): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .partition(|e| e.fanout.columns.len() == 1);
    #[allow(clippy::cast_precision_loss)]
    let ratio = |v: f64, total: u64| if total == 0 { 0.0 } else { v / total as f64 };
    tuples.sort_by(|a, b| {
        let (fa, fb) = (&a.fanout, &b.fanout);
        ratio(fa.files_per_value[1], fa.files_total)
            .total_cmp(&ratio(fb.files_per_value[1], fb.files_total))
            .then(
                ratio(fa.row_groups_per_value[1], fa.row_groups_total)
                    .total_cmp(&ratio(fb.row_groups_per_value[1], fb.row_groups_total)),
            )
    });
    singles.into_iter().chain(tuples).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the cached fanout layout: a change to [`FanoutTable`] or
    /// [`CachedEntry`] that alters these bytes must bump
    /// [`FANOUT_CACHE_FORMAT`] and this golden together.
    #[test]
    fn cached_fanout_layout_is_pinned_to_its_format() {
        let table = FanoutTable {
            format: FANOUT_CACHE_FORMAT.to_string(),
            entries: vec![CachedEntry {
                columns: vec!["a".into(), "b".into()],
                distinct: 9,
                sampled: false,
                files: [1, 2, 3],
                row_groups: [1, 2, 4],
                files_total: 5,
                row_groups_total: 10,
                retained: 9,
                admission_bound: "ffffffffffffffffffffffffffffffff".into(),
                hll_estimate: 9,
                distinct_source: DistinctSource::Exact,
                peak_entries: 9,
                peak_capacity: 14,
                high_water: 14,
                capped_at_rows: false,
            }],
            memory: MemoryReport {
                keys: 1,
                high_water: 14,
                tables_bound_bytes: 792,
                sketches_bytes: 16_384,
                peak_capacity: 14,
                max_row_group_rows: 3,
            },
        };
        let golden = r#"{"format":"bellwether-profile/fanout/3","entries":[{"columns":["a","b"],"distinct":9,"sampled":false,"files":[1,2,3],"row_groups":[1,2,4],"files_total":5,"row_groups_total":10,"retained":9,"admission_bound":"ffffffffffffffffffffffffffffffff","hll_estimate":9,"distinct_source":"exact","peak_entries":9,"peak_capacity":14,"high_water":14,"capped_at_rows":false}],"memory":{"keys":1,"high_water":14,"tables_bound_bytes":792,"sketches_bytes":16384,"peak_capacity":14,"max_row_group_rows":3}}"#;
        assert_eq!(serde_json::to_string(&table).unwrap(), golden);
        assert_eq!(FANOUT_CACHE_FORMAT, "bellwether-profile/fanout/3");
        let back: FanoutTable = serde_json::from_str(golden).unwrap();
        assert_eq!(back, table);
    }
}
