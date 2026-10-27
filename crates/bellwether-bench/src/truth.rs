//! `ground-truth`: the planted anti-patterns, measured exactly.
//!
//! Each entry is a rule from `docs/rule-catalog.md`, with the fix an expert
//! would make written down before any engine runs (ADR 019), and a
//! `planted` flag **measured from the data**: a layout that does not exhibit
//! a pattern (one file per day, no late rows) records it as not planted
//! rather than claiming it. The thresholds are the `*_MIN_*` constants below
//! and are recorded in `docs/benchmark.md`.
//!
//! The two decisions a lint also makes are not reimplemented here.
//! `scattered_time_column` is `bellwether_source::stats::overlap_depth` of
//! `version_ts`, and `batch_stamp_key` is `bellwether_source::stats::batch_stamp`
//! (its files gate, its ratio against `(entity_id, batch_id)`, and its
//! cross-day concordance with time), both over the corpus's own footer
//! statistics read through `bellwether_source`, at the thresholds defined
//! there. The profiler's lints call the same functions: the scatter flag and
//! its lint agree by construction. The composite partner is chosen by
//! `bellwether_source::stats::partner_order` here and there; here the only
//! candidate is `entity_id`, which the benchmark's workload supplies, and the
//! profiler run with that workload considers only it too, so the batch-stamp
//! and low-selectivity flags agree with their lints by construction.
//!
//! Fanout is counted exactly without sets: rows are visited in one fixed
//! order of row groups, and per value the scan keeps the last row group and
//! file seen and increments a counter when either changes.
//!
//! Memory (ADR 018): `entity_id` state is `O(entities)` (20 bytes per
//! entity), reused for the five `(status, entity_id)` passes. `batch_id` and
//! `(entity_id, batch_id)` state is held for one day at a time, which is
//! exact because `batch_id` is a write-time stamp and never crosses a day
//! (checked during the scan). Footer statistics are `O(row groups)`.
//! Distributions are `value -> count` histograms.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use arrow::array::{
    Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use bellwether_source::stats::{
    batch_stamp as shared_batch_stamp, overlap_depth, partner_order, OverlapDepth,
    PartnerCandidate, BATCH_MIN_FILES_P50, BATCH_MIN_RATIO, BATCH_MIN_TIME_CORRELATION,
    SCATTER_MAX_SORTEDNESS, SCATTER_MIN_DEPTH,
};
use bellwether_source::{fingerprint, ParquetSource};
use bellwether_types::{Fanout, Hash, KeySpec, RewriteShape, RowGroupMeta, Source};
use serde::{Deserialize, Serialize};

use crate::corpus::{Corpus, COL_BATCH, COL_ENTITY, COL_STATUS, COL_TS};
use crate::stats::{Bucket, CountHistogram};
use crate::workload::WORKLOAD_KEY_COLUMNS;
use crate::{write_json, BenchError, Result};

/// Format tag of `expected.json`.
pub const EXPECTED_FORMAT: &str = "bellwether-bench-expected/3";
/// Rule: a column that looks like a key but is a batch stamp.
pub const RULE_BATCH_STAMP: &str = "batch_stamp_key";
/// Rule: a single-column key whose values each touch most row groups.
pub const RULE_LOW_SELECTIVITY: &str = "low_selectivity_single_column";
/// Rule: a time column whose row-group ranges overlap far beyond its rows.
pub const RULE_SCATTERED_TIME: &str = "scattered_time_column";
/// Rule: reading full history for the latest version per key.
pub const RULE_LOOKBACK: &str = "lookback_unbounded";
/// Rule: a declared key spec no workload uses (deferred).
pub const RULE_DECLARED_UNUSED: &str = "declared_but_unused_key";

/// `low_selectivity_single_column` is planted when the median value touches
/// at least this fraction of all row groups...
pub const LOW_SELECTIVITY_MIN_FRACTION: f64 = 0.5;
/// ...and that median is at least this many times the fix composite's p95.
pub const LOW_SELECTIVITY_MIN_RATIO_OVER_FIX: f64 = 2.0;
/// `lookback_unbounded` is planted when the p95 entity's history spans at
/// least this many row groups.
pub const LOOKBACK_MIN_RG_FANOUT_P95: f64 = 2.0;

/// `expected.json`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Expected {
    /// [`EXPECTED_FORMAT`].
    pub format: String,
    /// Corpus measured.
    pub corpus_id: Hash,
    /// Quantile definition used throughout.
    pub quantiles: String,
    /// Rows scanned.
    pub rows_total: u64,
    /// Files in the corpus.
    pub files_total: u64,
    /// Row groups in the corpus.
    pub row_groups_total: u64,
    /// One entry per catalog rule the benchmark plants or defers.
    pub anti_patterns: Vec<AntiPattern>,
}

impl Expected {
    /// The entry for this rule id.
    #[must_use]
    pub fn get(&self, rule: &str) -> Option<&AntiPattern> {
        self.anti_patterns.iter().find(|a| a.rule == rule)
    }
}

/// One catalog rule, measured on this corpus.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AntiPattern {
    /// Rule id from `docs/rule-catalog.md`.
    pub rule: String,
    /// Columns involved, in prefix order.
    pub columns: Vec<String>,
    /// Whether the data exhibits the pattern, by the measured criterion.
    pub planted: bool,
    /// The criterion with this corpus's numbers.
    pub criterion: String,
    /// Why the pattern cannot be scored yet, naming the missing input. The
    /// measured `planted` value is kept either way.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub deferred: Option<String>,
    /// One sentence a non-specialist can act on.
    pub finding: String,
    /// Exact fanout of `columns`; `sampled` is always false.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fanout: Option<Fanout>,
    /// Values per power-of-two bucket of row groups touched.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub row_groups_histogram: Vec<Bucket>,
    /// The expert fix, in the catalog's guidance-template shape.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fix: Option<RewriteShape>,
    /// Exact fanout of the key the fix introduces, when it introduces one.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fix_fanout: Option<Fanout>,
    /// The index spec the fix implies, if any.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fix_key_spec: Option<KeySpec>,
    /// The clustering the fix implies, if any.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fix_layout: Option<Vec<String>>,
    /// Version detail for [`RULE_LOOKBACK`].
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub versions: Option<VersionDetail>,
    /// Range detail for [`RULE_SCATTERED_TIME`].
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub time_scatter: Option<TimeScatter>,
}

/// How versions of an entity relate to its fanout.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionDetail {
    /// Entities with at least one version.
    pub entities: u64,
    /// Versions (rows) per entity at p50, p95, max.
    pub versions_per_entity: [f64; 3],
    /// Mean versions per entity (the `churn` of `docs/benchmark.md`).
    pub mean_versions_per_entity: f64,
    /// Entities whose row-group fanout equals their version count.
    pub entities_fanout_equals_versions: u64,
}

/// How scattered `version_ts` is, by the shared overlap-depth measure
/// (`bellwether_source::stats::overlap_depth`) over the corpus's footer
/// statistics, with the late rows the generator wrote for reference.
///
/// `depth` is the time-weighted mean number of row-group ranges containing a
/// point of their union, after excluding sentinel ranges; `sortedness` is the
/// fraction of consecutive within-file row groups whose ranges ascend without
/// overlapping. The pattern is planted when depth is at least
/// `SCATTER_MIN_DEPTH` and sortedness is below `SCATTER_MAX_SORTEDNESS`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TimeScatter {
    /// Overlap depth.
    pub depth: f64,
    /// Sortedness.
    pub sortedness: f64,
    /// Sum of the measured ranges' lengths, microseconds.
    pub covered: u64,
    /// Length of their union, microseconds.
    pub union: u64,
    /// Consecutive within-file pairs whose ranges ascend without overlap.
    pub sorted_pairs: u64,
    /// Consecutive within-file pairs measured.
    pub pairs: u64,
    /// Ranges measured.
    pub ranges: u64,
    /// Ranges excluded as sentinels.
    pub excluded: u64,
    /// Rows whose `version_ts` precedes their file's day.
    pub late_rows: u64,
    /// `late_rows / rows_total`.
    pub late_fraction: f64,
}

#[derive(Clone, Copy)]
struct Track {
    last_rg: u32,
    last_file: u32,
    rgs: u32,
    files: u32,
    rows: u32,
}

impl Track {
    const NEW: Self = Self {
        last_rg: u32::MAX,
        last_file: u32::MAX,
        rgs: 0,
        files: 0,
        rows: 0,
    };

    fn touch(&mut self, rg: u32, file: u32) {
        self.rows += 1;
        if self.last_rg != rg {
            self.last_rg = rg;
            self.rgs += 1;
        }
        if self.last_file != file {
            self.last_file = file;
            self.files += 1;
        }
    }
}

#[derive(Default)]
struct Dist {
    rgs: CountHistogram,
    files: CountHistogram,
}

impl Dist {
    fn add(&mut self, t: &Track) {
        self.rgs.add(u64::from(t.rgs));
        self.files.add(u64::from(t.files));
    }

    fn fanout(&self, columns: &[&str], files_total: u64, row_groups_total: u64) -> Fanout {
        Fanout {
            columns: columns.iter().map(|c| (*c).to_string()).collect(),
            distinct: self.rgs.total(),
            sampled: false,
            files_per_value: self.files.p50_p95_max(),
            row_groups_per_value: self.rgs.p50_p95_max(),
            files_total,
            row_groups_total,
            sample_size: None,
            quantile_error: None,
        }
    }
}

fn u64_col<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| BenchError::Invalid(format!("{name} is not uint64")))
}

fn i64_col<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a Int64Array> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| BenchError::Invalid(format!("{name} is not int64")))
}

fn str_col<'a>(batch: &'a RecordBatch, idx: usize, name: &str) -> Result<&'a StringArray> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| BenchError::Invalid(format!("{name} is not utf8")))
}

fn entity_slot(entity: u64, len: usize) -> Result<usize> {
    usize::try_from(entity)
        .ok()
        .filter(|&s| s < len)
        .ok_or_else(|| BenchError::Invalid(format!("entity_id {entity} out of range")))
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|c| (*c).to_string()).collect()
}

/// Everything the main pass measures.
struct MainPass {
    entity: Vec<Track>,
    batch: Dist,
    composite: Dist,
    status: BTreeMap<String, Track>,
    rows_total: u64,
    late_rows: u64,
}

fn main_pass(corpus: &Corpus) -> Result<MainPass> {
    let params = &corpus.manifest.params;
    let mut out = MainPass {
        entity: vec![Track::NEW; params.entities as usize],
        batch: Dist::default(),
        composite: Dist::default(),
        status: BTreeMap::new(),
        rows_total: 0,
        late_rows: 0,
    };
    let projection = [COL_ENTITY, COL_BATCH, COL_TS, COL_STATUS];
    let mut global_rg = 0u32;
    for day in &corpus.manifest.days {
        let mut batch_day: HashMap<i64, Track> = HashMap::new();
        let mut composite_day: HashMap<(u64, i64), Track> = HashMap::new();
        let day_start = day.start_us();
        let day_stamp: i64 =
            day.date.replace('-', "").parse().map_err(|_| {
                BenchError::Invalid(format!("bad date {:?} in corpus.json", day.date))
            })?;
        for file_idx in day.first_file..day.first_file + day.files {
            let meta = &corpus.files[file_idx as usize];
            for rg in 0..meta.row_groups.len() {
                let (batches, _) = meta.reader.read(rg, Some(&projection))?;
                for batch in batches {
                    let e = u64_col(&batch, 0, "entity_id")?;
                    let b = i64_col(&batch, 1, "batch_id")?;
                    let t = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| BenchError::Invalid("version_ts type".into()))?;
                    let st = str_col(&batch, 3, "status")?;
                    for i in 0..batch.num_rows() {
                        let (ev, bv, tv) = (e.value(i), b.value(i), t.value(i));
                        let slot = entity_slot(ev, out.entity.len())?;
                        if bv / 10_000 != day_stamp {
                            return Err(BenchError::Invalid(format!(
                                "batch_id {bv} crosses into day {}",
                                day.date
                            )));
                        }
                        out.entity[slot].touch(global_rg, file_idx);
                        batch_day
                            .entry(bv)
                            .or_insert(Track::NEW)
                            .touch(global_rg, file_idx);
                        composite_day
                            .entry((ev, bv))
                            .or_insert(Track::NEW)
                            .touch(global_rg, file_idx);
                        let sv = st.value(i);
                        if let Some(track) = out.status.get_mut(sv) {
                            track.touch(global_rg, file_idx);
                        } else {
                            let mut track = Track::NEW;
                            track.touch(global_rg, file_idx);
                            out.status.insert(sv.to_string(), track);
                        }
                        if tv < day_start {
                            out.late_rows += 1;
                        }
                        out.rows_total += 1;
                    }
                }
                global_rg += 1;
            }
        }
        for t in batch_day.values() {
            out.batch.add(t);
        }
        for t in composite_day.values() {
            out.composite.add(t);
        }
    }
    Ok(out)
}

/// Exact fanout of `(status, entity_id)`: one pass per status value over
/// the two columns, reusing one `O(entities)` tracker.
fn status_entity_pass(corpus: &Corpus, statuses: &[String], tracker: &mut [Track]) -> Result<Dist> {
    let mut dist = Dist::default();
    let projection = [COL_ENTITY, COL_STATUS];
    for status in statuses {
        tracker.fill(Track::NEW);
        let mut global_rg = 0u32;
        for (file_idx, meta) in corpus.files.iter().enumerate() {
            for rg in 0..meta.row_groups.len() {
                let (batches, _) = meta.reader.read(rg, Some(&projection))?;
                for batch in batches {
                    let e = u64_col(&batch, 0, "entity_id")?;
                    let st = str_col(&batch, 1, "status")?;
                    for i in 0..batch.num_rows() {
                        if st.value(i) == status {
                            let slot = entity_slot(e.value(i), tracker.len())?;
                            tracker[slot].touch(global_rg, file_idx as u32);
                        }
                    }
                }
                global_rg += 1;
            }
        }
        for t in tracker.iter().filter(|t| t.rows > 0) {
            dist.add(t);
        }
    }
    Ok(dist)
}

/// Every row group of the corpus as `bellwether_source` reports it, files in
/// manifest (path) order: the footer statistics the shared measures read.
fn footer_row_groups(corpus: &Corpus) -> Result<Vec<RowGroupMeta>> {
    let source = ParquetSource::new();
    let mut out = Vec::with_capacity(corpus.row_groups_total() as usize);
    for meta in &corpus.files {
        let path = meta.path.to_str().ok_or_else(|| {
            BenchError::Invalid(format!("{}: path is not UTF-8", meta.path.display()))
        })?;
        let fp = fingerprint(path).map_err(|e| BenchError::Invalid(e.to_string()))?;
        out.extend(
            source
                .row_groups(&fp)
                .map_err(|e| BenchError::Invalid(e.to_string()))?,
        );
    }
    Ok(out)
}

/// Scans the corpus and measures every catalog rule the benchmark plants.
pub fn compute(corpus: &Corpus) -> Result<Expected> {
    let files_total = corpus.files.len() as u64;
    let row_groups_total = corpus.row_groups_total();
    let mut main = main_pass(corpus)?;
    let footers = footer_row_groups(corpus)?;
    let shared = |e: bellwether_types::Error| BenchError::Invalid(e.to_string());
    let overlap = overlap_depth(&footers, "version_ts").map_err(shared)?;

    let mut entity_dist = Dist::default();
    let mut versions = CountHistogram::default();
    let mut equal = 0u64;
    let mut version_rows = 0u64;
    for t in main.entity.iter().filter(|t| t.rows > 0) {
        entity_dist.add(t);
        versions.add(u64::from(t.rows));
        version_rows += u64::from(t.rows);
        equal += u64::from(t.rgs == t.rows);
    }
    let mut status_dist = Dist::default();
    for t in main.status.values() {
        status_dist.add(t);
    }
    let statuses: Vec<String> = main.status.keys().cloned().collect();
    let status_entity = status_entity_pass(corpus, &statuses, &mut main.entity)?;

    let anti_patterns = vec![
        batch_stamp(
            &main,
            files_total,
            row_groups_total,
            corpus,
            &footers,
            entity_dist.rgs.total(),
        )?,
        low_selectivity(
            &status_dist,
            &status_entity,
            files_total,
            row_groups_total,
            entity_dist.rgs.total(),
            main.rows_total,
        ),
        scattered_time(&overlap, main.late_rows, main.rows_total)?,
        lookback(
            &entity_dist,
            &versions,
            equal,
            version_rows,
            files_total,
            row_groups_total,
        ),
        declared_unused(),
    ];

    Ok(Expected {
        format: EXPECTED_FORMAT.into(),
        corpus_id: corpus.id,
        quantiles: "nearest-rank: the q-quantile of n sorted values is the value at zero-based \
                    index ceil(q*n)-1"
            .into(),
        rows_total: main.rows_total,
        files_total,
        row_groups_total,
        anti_patterns,
    })
}

fn batch_stamp(
    main: &MainPass,
    files_total: u64,
    rgs_total: u64,
    corpus: &Corpus,
    footers: &[RowGroupMeta],
    entity_distinct: u64,
) -> Result<AntiPattern> {
    let fanout = main.batch.fanout(&["batch_id"], files_total, rgs_total);
    let fix_fanout = main
        .composite
        .fanout(&["entity_id", "batch_id"], files_total, rgs_total);
    let files_p50 = fanout.files_per_value[0];
    let rgs_p50 = fanout.row_groups_per_value[0];
    let fix_p50 = fix_fanout.row_groups_per_value[0];
    let decision = shared_batch_stamp(
        files_p50,
        rgs_p50,
        fix_p50,
        footers,
        "batch_id",
        "version_ts",
    )
    .map_err(|e| BenchError::Invalid(e.to_string()))?;
    // The partner rule the profiler applies, from the same inputs: the fix's
    // partner must not be refused as an unsupplied row id.
    let partner_ok = entity_partner_ok(
        entity_distinct,
        main.rows_total,
        fix_fanout.row_groups_per_value[1],
        entity_supplied(),
    );
    let planted = decision.planted && partner_ok;
    let concordance = decision.concordance;
    let in_time = concordance.is_some_and(|c| c >= BATCH_MIN_TIME_CORRELATION);
    let concordance_text =
        concordance.map_or_else(|| "undefined (one day)".to_string(), |c| format!("{c:.3}"));
    let files_per_day = corpus
        .manifest
        .days
        .iter()
        .map(|d| d.files)
        .max()
        .unwrap_or(0);
    let finding = if planted {
        format!(
            "batch_id has {} distinct values and looks like an id, but it is a 20-minute write \
             stamp: one value touches {files_p50} files (a day has up to {files_per_day}) and \
             {rgs_p50} row groups at the median, against {fix_p50} for (entity_id, batch_id), so \
             a lookup by it reads {:.0}x more.",
            fanout.distinct,
            rgs_p50 / fix_p50.max(1.0)
        )
    } else if !in_time {
        format!(
            "batch_id is a write stamp, but its cross-day concordance with time is \
             {concordance_text}, so in this layout the pattern is not planted."
        )
    } else {
        format!(
            "batch_id is a write stamp, but in this layout one value touches only {files_p50} \
             file(s) and {rgs_p50} row group(s) at the median, so the pattern is not planted."
        )
    };
    Ok(AntiPattern {
        rule: RULE_BATCH_STAMP.into(),
        columns: strings(&["batch_id"]),
        planted,
        criterion: format!(
            "p50 files per value {files_p50} >= {BATCH_MIN_FILES_P50}, p50 row groups per \
             value {rgs_p50} >= {BATCH_MIN_RATIO} x fix p50 {fix_p50}, and cross-day \
             concordance with version_ts {concordance_text} >= {BATCH_MIN_TIME_CORRELATION}"
        ),
        deferred: None,
        finding,
        row_groups_histogram: main.batch.rgs.log2_buckets(),
        fanout: Some(fanout),
        fix: Some(RewriteShape {
            pairs: Vec::new(),
            key: Vec::new(),
            keys: None,
            before: "join on batch_id".into(),
            after: "join on (entity_id, batch_id)".into(),
            invariant: "the set of output rows per entity_id is unchanged".into(),
            before_plan: None,
            after_plan: None,
        }),
        fix_fanout: Some(fix_fanout),
        fix_key_spec: Some(KeySpec::Exact {
            columns: strings(&["entity_id", "batch_id"]),
            counts: false,
            bounds: None,
        }),
        fix_layout: None,
        versions: None,
        time_scatter: None,
    })
}

/// Whether the benchmark's workload supplies `entity_id`
/// ([`WORKLOAD_KEY_COLUMNS`]); it always does, and a test pins that.
fn entity_supplied() -> bool {
    WORKLOAD_KEY_COLUMNS.contains(&"entity_id")
}

/// Whether `entity_id` survives the shared partner rule
/// (`bellwether_source::stats::partner_order`) as the fix's partner, given
/// whether the workload supplies it. Supplied, even a fully unique
/// `entity_id` is the workload's key, not a refused row id.
fn entity_partner_ok(
    entity_distinct: u64,
    rows_total: u64,
    composite_p95: f64,
    supplied: bool,
) -> bool {
    let candidate = PartnerCandidate {
        column: "entity_id".into(),
        distinct: entity_distinct,
        distinct_exact: true,
        supplied,
        composite_row_groups_p95: composite_p95,
    };
    !partner_order(&[candidate], rows_total).is_empty()
}

fn low_selectivity(
    status: &Dist,
    fix: &Dist,
    files_total: u64,
    rgs_total: u64,
    entity_distinct: u64,
    rows_total: u64,
) -> AntiPattern {
    let fanout = status.fanout(&["status"], files_total, rgs_total);
    let fix_fanout = fix.fanout(&["status", "entity_id"], files_total, rgs_total);
    let p50 = fanout.row_groups_per_value[0];
    let fraction = if rgs_total == 0 {
        0.0
    } else {
        p50 / rgs_total as f64
    };
    let fix_p95 = fix_fanout.row_groups_per_value[1];
    let planted = fraction >= LOW_SELECTIVITY_MIN_FRACTION
        && p50 >= LOW_SELECTIVITY_MIN_RATIO_OVER_FIX * fix_p95
        && entity_partner_ok(entity_distinct, rows_total, fix_p95, entity_supplied());
    let finding = if planted {
        format!(
            "status has {} values and the median one touches {p50} of {rgs_total} row groups \
             ({:.0}%): as a key it prunes almost nothing, while (status, entity_id) touches at \
             most {fix_p95} at p95.",
            fanout.distinct,
            fraction * 100.0
        )
    } else {
        format!(
            "status's median value touches {p50} of {rgs_total} row groups ({:.0}%), so the \
             pattern is not planted.",
            fraction * 100.0
        )
    };
    AntiPattern {
        rule: RULE_LOW_SELECTIVITY.into(),
        columns: strings(&["status"]),
        planted,
        criterion: format!(
            "p50 row groups per value / row groups total {fraction:.3} >= \
             {LOW_SELECTIVITY_MIN_FRACTION} and p50 {p50} >= {LOW_SELECTIVITY_MIN_RATIO_OVER_FIX} \
             x fix p95 {fix_p95}"
        ),
        deferred: Some(
            "needs a recorded workload whose runs supply status, and a declared key spec on \
             status; every recorded run supplies only entity_id and no key spec on status is \
             declared, so the fix key [status, entity_id] is unusable"
                .into(),
        ),
        finding,
        row_groups_histogram: status.rgs.log2_buckets(),
        fanout: Some(fanout),
        fix: Some(RewriteShape {
            pairs: Vec::new(),
            key: Vec::new(),
            keys: None,
            before: "key = [status]".into(),
            after: "key = [status, entity_id]".into(),
            invariant: "every row group holding a queried key is still selected (invariant 1)"
                .into(),
            before_plan: None,
            after_plan: None,
        }),
        fix_fanout: Some(fix_fanout),
        fix_key_spec: Some(KeySpec::Exact {
            columns: strings(&["status", "entity_id"]),
            counts: false,
            bounds: None,
        }),
        fix_layout: None,
        versions: None,
        time_scatter: None,
    }
}

fn scattered_time(overlap: &OverlapDepth, late_rows: u64, rows_total: u64) -> Result<AntiPattern> {
    let planted = overlap.is_scattered();
    let micros = |v: u128| {
        u64::try_from(v)
            .map_err(|_| BenchError::Invalid(format!("version_ts range total {v} overflows u64")))
    };
    let scatter = TimeScatter {
        depth: overlap.depth,
        sortedness: overlap.sortedness,
        covered: micros(overlap.covered)?,
        union: micros(overlap.union)?,
        sorted_pairs: overlap.sorted_pairs,
        pairs: overlap.pairs,
        ranges: overlap.ranges,
        excluded: overlap.excluded,
        late_rows,
        late_fraction: if rows_total == 0 {
            0.0
        } else {
            late_rows as f64 / rows_total as f64
        },
    };
    let finding = if planted {
        format!(
            "version_ts is out of order: a point in time falls in {:.1} row-group ranges on \
             average, and only {:.0}% of consecutive row groups in a file ascend without \
             overlapping, so min/max pruning on time reads far more than it needs.",
            scatter.depth,
            scatter.sortedness * 100.0
        )
    } else {
        format!(
            "version_ts ranges have overlap depth {:.2} and sortedness {:.2}, so the pattern is \
             not planted.",
            scatter.depth, scatter.sortedness
        )
    };
    Ok(AntiPattern {
        rule: RULE_SCATTERED_TIME.into(),
        columns: strings(&["version_ts"]),
        planted,
        criterion: format!(
            "overlap depth {:.3} >= {SCATTER_MIN_DEPTH} and sortedness {:.3} < \
             {SCATTER_MAX_SORTEDNESS}",
            scatter.depth, scatter.sortedness
        ),
        deferred: None,
        finding,
        fanout: None,
        row_groups_histogram: Vec::new(),
        fix: Some(RewriteShape {
            pairs: Vec::new(),
            key: Vec::new(),
            keys: None,
            before: "written in arrival order".into(),
            after: "sorted or clustered by version_ts within each partition".into(),
            invariant: "the multiset of rows is unchanged".into(),
            before_plan: None,
            after_plan: None,
        }),
        fix_fanout: None,
        fix_key_spec: None,
        fix_layout: Some(strings(&["version_ts"])),
        versions: None,
        time_scatter: Some(scatter),
    })
}

fn lookback(
    entity: &Dist,
    versions: &CountHistogram,
    equal: u64,
    version_rows: u64,
    files_total: u64,
    rgs_total: u64,
) -> AntiPattern {
    let fanout = entity.fanout(&["entity_id"], files_total, rgs_total);
    let p95 = fanout.row_groups_per_value[1];
    let planted = p95 >= LOOKBACK_MIN_RG_FANOUT_P95;
    let mean = if versions.total() == 0 {
        0.0
    } else {
        version_rows as f64 / versions.total() as f64
    };
    let finding = if planted {
        format!(
            "entity_id is the true id, and its history is spread over time: the p95 entity's \
             versions sit in {p95} row groups (median {}, max {}), so reading full history to \
             find the latest version scans them all; a temporal index on entity_id by version_ts \
             locates the current version directly.",
            fanout.row_groups_per_value[0], fanout.row_groups_per_value[2]
        )
    } else {
        format!(
            "most entities have one version in one row group (p95 {p95}), so the pattern is \
             not planted."
        )
    };
    AntiPattern {
        rule: RULE_LOOKBACK.into(),
        columns: strings(&["entity_id"]),
        planted,
        criterion: format!("p95 row groups per entity {p95} >= {LOOKBACK_MIN_RG_FANOUT_P95}"),
        deferred: Some(
            "needs a consumer query plan with a latest-per-key step, which no record carries; \
             the recorded workload is locate-only"
                .into(),
        ),
        finding,
        row_groups_histogram: entity.rgs.log2_buckets(),
        fanout: Some(fanout),
        fix: Some(RewriteShape {
            pairs: Vec::new(),
            key: Vec::new(),
            keys: None,
            before: "scan all history, rank by version_ts per entity_id, keep first".into(),
            after: "latest(keys) via temporal index on entity_id by version_ts, then apply \
                    LatestPerKey residual"
                .into(),
            invariant: "the newest version per key is unchanged (invariant 5)".into(),
            before_plan: None,
            after_plan: None,
        }),
        fix_fanout: None,
        fix_key_spec: Some(KeySpec::Temporal {
            columns: strings(&["entity_id"]),
            ts_column: "version_ts".into(),
            counts: false,
            tombstone: None,
        }),
        fix_layout: None,
        versions: Some(VersionDetail {
            entities: versions.total(),
            versions_per_entity: versions.p50_p95_max(),
            mean_versions_per_entity: mean,
            entities_fanout_equals_versions: equal,
        }),
        time_scatter: None,
    }
}

fn declared_unused() -> AntiPattern {
    AntiPattern {
        rule: RULE_DECLARED_UNUSED.into(),
        columns: Vec::new(),
        planted: false,
        criterion: "needs a declared key spec that no recorded run uses".into(),
        deferred: Some(
            "the pattern lives in a manifest (a declared key spec), not in the data; it is \
             planted once a manifest fixture exists (WP3/WP4)"
                .into(),
        ),
        finding: "deferred: no manifest fixture declares key specs yet.".into(),
        fanout: None,
        row_groups_histogram: Vec::new(),
        fix: Some(RewriteShape {
            pairs: Vec::new(),
            key: Vec::new(),
            keys: None,
            before: "<spec name> declared in key_specs".into(),
            after: "removed".into(),
            invariant: "no recorded run's ScanPlan changes".into(),
            before_plan: None,
            after_plan: None,
        }),
        fix_fanout: None,
        fix_key_spec: None,
        fix_layout: None,
        versions: None,
        time_scatter: None,
    }
}

/// Computes and writes `corpus/expected.json`.
pub fn write_ground_truth(corpus_root: &Path) -> Result<Expected> {
    let corpus = Corpus::open(corpus_root)?;
    let expected = compute(&corpus)?;
    write_json(&corpus_root.join("expected.json"), &expected)?;
    Ok(expected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_workload_supplies_entity_id() {
        assert_eq!(WORKLOAD_KEY_COLUMNS, ["entity_id"]);
        assert!(entity_supplied());
    }

    #[test]
    fn a_row_id_partner_counts_only_when_supplied() {
        // A fully unique entity_id: the workload's key when supplied,
        // refused as a row id when not; a repeating one always counts.
        assert!(entity_partner_ok(1_000, 1_000, 1.0, true));
        assert!(!entity_partner_ok(1_000, 1_000, 1.0, false));
        assert!(entity_partner_ok(400, 1_000, 1.0, false));
    }
}
