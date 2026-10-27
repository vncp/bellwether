//! The lake twin of a query engine (ADR 033): `why --twin-of`.
//!
//! The diagnosis runs on the lake copy, never in the engine: one pass reads
//! the key sample's columns from every live row group of the lake (bounded
//! per row group, ADR 018) and counts, per candidate column set, the rows,
//! row groups, and files each sampled key value covers. Every number is
//! exact for the lake; the report's header says the engine's storage
//! differs. The engine is asked one thing only, its table listing, for the
//! identity the twin is compared against (freshness); no query text is
//! sent to it here.
//!
//! The four lints of the rule catalog's "Twin lints" fire from these
//! numbers. They are advice: they change no plan and propose nothing.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use arrow::datatypes::DataType;
use bellwether_profile::render::thousands;
use bellwether_replay::{Catalog, RunKeys, Workload};
use bellwether_source_engine::{EngineAdapter, EngineIdentity};
use bellwether_store::{Cache, ObjectStoreBackend};
use bellwether_types::{Error, Gate, Hash, KeySpec, Lint, Result, RewriteShape, Source, TwinOf};
use serde::{Deserialize, Serialize};

use crate::engine::Engine;

/// Rows per key value above which a key column is low-cardinality (lint
/// `low_cardinality_key`).
pub const LOW_CARDINALITY_ROWS: u64 = 100_000;
/// How many times fewer rows per value a co-occurring column must cover to
/// be the high-cardinality partner of a low-cardinality key.
pub const CARDINALITY_RATIO: u64 = 100;
/// The share of the twin's rows and of its bytes at or below which a key
/// projection is narrow (lint `wide_before_narrow`), in percent.
pub const NARROW_PERCENT: u64 = 10;
/// The share of the twin's files below which the key sample is selective
/// (lint `dedup_before_filter`), in percent.
pub const SELECTIVE_FILES_PERCENT: u64 = 1;
/// The gate name the membership lint reads.
pub const MEMBERSHIP_GATE: &str = "in";

/// One key column set, measured on the twin for the key sample.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinColumns {
    /// The columns, in the sample's order.
    pub columns: Vec<String>,
    /// Distinct sampled values projected onto these columns.
    pub keyset: u64,
    /// Rows of the twin each value covers (its fanout), nearest-rank p50,
    /// p95, max over the sampled values (a value the twin lacks counts 0).
    pub rows_per_key: [u64; 3],
    /// Files each value is in, p50, p95, max.
    pub files_per_key: [u64; 3],
    /// Row groups each value is in, p50, p95, max.
    pub row_groups_per_key: [u64; 3],
    /// Rows holding any sampled value.
    pub rows: u64,
    /// Row groups holding any sampled value.
    pub row_groups: u64,
    /// Files holding any sampled value.
    pub files: u64,
    /// Each gate, and whether the keyset fits under it.
    pub gates: Vec<GateFit>,
}

/// The keyset against one gate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateFit {
    /// The gate's name.
    pub name: String,
    /// Its limit.
    pub limit: u64,
    /// Whether the keyset is at most the limit.
    pub fits: bool,
}

/// One candidate key's twin columns.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TwinCandidate {
    /// The candidate.
    pub spec: KeySpec,
    /// Its columns' measurements, in any order of the sample's; `None` when
    /// the pass measured no set of exactly its columns: a column the
    /// sample lacks, or, for a sample of more than four columns, a subset
    /// that is neither a prefix nor a single column.
    pub twin: Option<TwinColumns>,
    /// The candidate's index-kind column, as the funnel shows it (the
    /// shared `KindColumn`), so an index kind's one number reads beside
    /// the twin's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind_column: Option<bellwether_types::KindColumn>,
}

/// What `why --twin-of` adds to the report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TwinReport {
    /// The header line: the diagnosis is on a copy.
    pub note: String,
    /// The engine side, exactly as the profile Evidence records it.
    pub of: TwinOf,
    /// The sample's own key columns, measured (absent without a workload).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<TwinColumns>,
    /// Every candidate of the funnel, with its columns measured.
    pub candidates: Vec<TwinCandidate>,
    /// The twin lints that fired (also in the profile Evidence).
    pub lints: Vec<Lint>,
    /// Row groups the twin pass read (the key columns only).
    pub row_groups_read: u64,
    /// Rows the twin pass read.
    pub rows_read: u64,
}

/// Counts for one sampled value under one column set.
#[derive(Clone, Copy, Debug, Default)]
struct KeyCount {
    rows: u64,
    row_groups: u64,
    files: u64,
    last_rg: Option<usize>,
    last_file: Option<usize>,
}

/// One column set being counted.
struct SetPass {
    positions: Vec<usize>,
    /// Probe bytes (both float zeros) to the value's index.
    probes: HashMap<Vec<u8>, usize>,
    counts: Vec<KeyCount>,
    rows: u64,
    row_groups: u64,
    files: u64,
    last_rg: Option<usize>,
    last_file: Option<usize>,
}

/// The pass's result per column set, and what it read.
pub(crate) struct TwinPass {
    pub sets: BTreeMap<Vec<String>, TwinColumns>,
    pub key: Vec<String>,
    pub row_groups_read: u64,
    pub rows_read: u64,
    pub keys_bytes: Option<(u64, u64)>,
    pub files_total: u64,
    pub rows_total: u64,
}

/// Nearest-rank p50, p95, max.
fn quantiles(mut v: Vec<u64>) -> [u64; 3] {
    if v.is_empty() {
        return [0; 3];
    }
    v.sort_unstable();
    let rank = |q: usize| {
        let n = v.len();
        let r = (q * n).div_ceil(100).max(1);
        v[r - 1]
    };
    [rank(50), rank(95), v[v.len() - 1]]
}

/// The column sets measured: the sample's whole key, every prefix, every
/// single column, and with at most four key columns every subset.
fn column_sets(width: usize) -> Vec<Vec<usize>> {
    let mut out: Vec<Vec<usize>> = Vec::new();
    let mut push = |s: Vec<usize>| {
        if !out.contains(&s) {
            out.push(s);
        }
    };
    push((0..width).collect());
    for n in 1..width {
        push((0..n).collect());
    }
    for j in 0..width {
        push(vec![j]);
    }
    if width <= 4 {
        for mask in 1u32..(1 << width) {
            push((0..width).filter(|j| mask & (1 << j) != 0).collect());
        }
    }
    out
}

/// One pass over the twin (module docs).
#[allow(clippy::too_many_lines)]
pub(crate) fn pass(
    source: &dyn Source,
    catalog: &Catalog,
    workload: &Workload,
    cache: &Cache<ObjectStoreBackend>,
    gates: &[Gate],
) -> Result<TwinPass> {
    let runs = workload.runs();
    let first = runs.first().ok_or_else(|| Error::InvalidArgument {
        field: "workload",
        reason: "the workload records no run, so there is no key sample to diagnose".into(),
    })?;
    let key = first.columns.clone();
    if runs.iter().any(|r| r.columns != key) {
        return Err(Error::InvalidArgument {
            field: "workload",
            reason: "a twin diagnosis reads one key sample, and the workload's runs look up \
                     different key columns; record the sample as one run (--from <keys.parquet>)"
                .into(),
        });
    }
    let schema = catalog.schema();
    let declared: Vec<DataType> = key
        .iter()
        .map(|c| {
            schema
                .field_with_name(c)
                .map(|f| f.data_type().clone())
                .map_err(|_| Error::UnknownColumn {
                    column: c.clone(),
                    root: String::new(),
                    columns: schema.fields().iter().map(|f| f.name().clone()).collect(),
                })
        })
        .collect::<Result<_>>()?;
    let all_keys: Vec<RunKeys> = runs
        .iter()
        .map(|r| workload.run_keys(cache, r))
        .collect::<Result<_>>()?;
    let mut sets: Vec<SetPass> = column_sets(key.len())
        .into_iter()
        .map(|positions| {
            let mut probes: HashMap<Vec<u8>, usize> = HashMap::new();
            let mut values: HashMap<Vec<u8>, usize> = HashMap::new();
            for rk in &all_keys {
                for i in 0..rk.len() {
                    let mut projected = Vec::new();
                    for &j in &positions {
                        projected.extend_from_slice(rk.element(i, j));
                    }
                    let n = values.len();
                    let id = *values.entry(projected).or_insert(n);
                    for p in rk.probes(i, &positions) {
                        probes.insert(p, id);
                    }
                }
            }
            SetPass {
                positions,
                probes,
                counts: vec![KeyCount::default(); values.len()],
                rows: 0,
                row_groups: 0,
                files: 0,
                last_rg: None,
                last_file: None,
            }
        })
        .collect();
    let mut order: Vec<usize> = (0..catalog.row_groups().len()).collect();
    order.sort_by_key(|&i| (catalog.row_groups()[i].file, catalog.row_groups()[i].id));
    let (mut row_groups_read, mut rows_read) = (0u64, 0u64);
    let (mut key_bytes, mut all_bytes, mut bytes_known) = (0u64, 0u64, true);
    for (rg_idx, &i) in order.iter().enumerate() {
        let rg = &catalog.row_groups()[i];
        for s in &rg.meta.stats {
            match s.bytes {
                Some(b) => {
                    all_bytes += b;
                    if key.contains(&s.column) {
                        key_bytes += b;
                    }
                }
                None => bytes_known = false,
            }
        }
        let batch = source.read_columns(&rg.meta, &key)?;
        row_groups_read += 1;
        rows_read += batch.num_rows() as u64;
        let arrays: Vec<_> = key
            .iter()
            .map(|c| {
                batch.column_by_name(c).cloned().ok_or_else(|| {
                    Error::Source(format!(
                        "a read of {} lacks column {c:?}",
                        rg.meta.file.path
                    ))
                })
            })
            .collect::<Result<_>>()?;
        let mut elements: Vec<Option<Vec<u8>>> = vec![None; key.len()];
        for row in 0..batch.num_rows() {
            for (j, (a, dt)) in arrays.iter().zip(&declared).enumerate() {
                let mut buf = Vec::new();
                elements[j] = if bellwether_core::encoding::encode_array_value_as(
                    a.as_ref(),
                    row,
                    dt,
                    &mut buf,
                )? {
                    Some(buf)
                } else {
                    None
                };
            }
            for s in &mut sets {
                let mut probe = Vec::new();
                let mut null = false;
                for &j in &s.positions {
                    let Some(e) = &elements[j] else {
                        null = true;
                        break;
                    };
                    probe.extend_from_slice(e);
                }
                // A tuple holding a null matches nothing (ADR 014).
                if null {
                    continue;
                }
                let Some(&id) = s.probes.get(&probe) else {
                    continue;
                };
                let c = &mut s.counts[id];
                c.rows += 1;
                if c.last_rg != Some(rg_idx) {
                    c.last_rg = Some(rg_idx);
                    c.row_groups += 1;
                }
                if c.last_file != Some(rg.file) {
                    c.last_file = Some(rg.file);
                    c.files += 1;
                }
                s.rows += 1;
                if s.last_rg != Some(rg_idx) {
                    s.last_rg = Some(rg_idx);
                    s.row_groups += 1;
                }
                if s.last_file != Some(rg.file) {
                    s.last_file = Some(rg.file);
                    s.files += 1;
                }
            }
        }
    }
    let mut out = BTreeMap::new();
    for s in sets {
        let columns: Vec<String> = s.positions.iter().map(|&j| key[j].clone()).collect();
        let keyset = s.counts.len() as u64;
        out.insert(
            columns.clone(),
            TwinColumns {
                columns,
                keyset,
                rows_per_key: quantiles(s.counts.iter().map(|c| c.rows).collect()),
                files_per_key: quantiles(s.counts.iter().map(|c| c.files).collect()),
                row_groups_per_key: quantiles(s.counts.iter().map(|c| c.row_groups).collect()),
                rows: s.rows,
                row_groups: s.row_groups,
                files: s.files,
                gates: gates
                    .iter()
                    .map(|g| GateFit {
                        name: g.name.clone(),
                        limit: g.limit,
                        fits: keyset <= g.limit,
                    })
                    .collect(),
            },
        );
    }
    Ok(TwinPass {
        sets: out,
        key,
        row_groups_read,
        rows_read,
        keys_bytes: bytes_known.then_some((key_bytes, all_bytes)),
        files_total: catalog.files().len() as u64,
        rows_total: catalog.rows_total(),
    })
}

/// The pass's measurements of `columns`, whatever their order in the
/// sample (no count depends on it); `None` when the pass measured no set
/// of exactly these columns.
pub(crate) fn measured(p: &TwinPass, columns: &[String]) -> Option<TwinColumns> {
    if let Some(c) = p.sets.get(columns) {
        return Some(c.clone());
    }
    let mut want = columns.to_vec();
    want.sort();
    p.sets
        .iter()
        .find(|(k, _)| {
            let mut k = (*k).clone();
            k.sort();
            k == want
        })
        .map(|(_, c)| TwinColumns {
            columns: columns.to_vec(),
            ..c.clone()
        })
}

fn lint(
    rule: &str,
    columns: Vec<String>,
    message: String,
    fresh: bool,
    [before, after, invariant]: [String; 3],
) -> Lint {
    Lint {
        rule: rule.to_string(),
        columns,
        message,
        // Rule catalog, "Twin lints": exact counts over the whole twin are
        // high (0.9) while the twin holds the engine's row count, medium
        // (0.6) when it does not.
        confidence: if fresh { 0.9 } else { 0.6 },
        site: None,
        // The rule catalog's guidance template, "Twin lints": a described
        // rewrite of the agent's engine query, never one the engine runs.
        fix: Some(RewriteShape {
            before,
            after,
            invariant,
            pairs: Vec::new(),
            key: Vec::new(),
            keys: None,
            before_plan: None,
            after_plan: None,
        }),
    }
}

/// The four twin lints (rule catalog, "Twin lints"), from the pass.
#[allow(clippy::too_many_lines)]
pub(crate) fn lints(p: &TwinPass, gates: &[Gate], latest_per_key: bool, fresh: bool) -> Vec<Lint> {
    let mut out = Vec::new();
    // 1. low-cardinality key.
    let singles: Vec<&TwinColumns> = p
        .key
        .iter()
        .filter_map(|c| p.sets.get(&vec![c.clone()]))
        .collect();
    for low in &singles {
        if low.rows_per_key[0] <= LOW_CARDINALITY_ROWS {
            continue;
        }
        if let Some(high) = singles.iter().find(|h| {
            h.columns != low.columns
                && h.rows_per_key[0].saturating_mul(CARDINALITY_RATIO) <= low.rows_per_key[0]
        }) {
            out.push(lint(
                "low_cardinality_key",
                vec![low.columns[0].clone(), high.columns[0].clone()],
                format!(
                    "{} is a low-cardinality key: each sampled value covers {} rows (median) of \
                     the twin, while {} covers {}; a lookup by {} alone reads every row of each \
                     value, so key it by the composite ({}, {})",
                    low.columns[0],
                    thousands(low.rows_per_key[0]),
                    high.columns[0],
                    thousands(high.rows_per_key[0]),
                    low.columns[0],
                    high.columns[0],
                    low.columns[0]
                ),
                fresh,
                [
                    format!("filter or join by {}", low.columns[0]),
                    format!(
                        "filter or join by ({}, {})",
                        high.columns[0], low.columns[0]
                    ),
                    "the rows selected are those whose (partner, column) is in the sample".into(),
                ],
            ));
        }
    }
    let Some(whole) = p.sets.get(&p.key) else {
        return out;
    };
    // 2. unbounded membership.
    if let Some(g) = gates.iter().find(|g| g.name == MEMBERSHIP_GATE) {
        if whole.keyset > g.limit {
            out.push(lint(
                "unbounded_membership",
                p.key.clone(),
                format!(
                    "the key sample holds {} distinct keys, above the membership gate of {}: a \
                     membership list of that size fails or degrades; count the keys first and \
                     branch, a membership list under the gate and a join above it",
                    thousands(whole.keyset),
                    thousands(g.limit)
                ),
                fresh,
                [
                    format!("a membership list of {} keys", thousands(whole.keyset)),
                    "count the keys first: a membership list under the gate, a join above it"
                        .into(),
                    "both branches select the same rows".into(),
                ],
            ));
        }
    }
    // 3. wide-before-narrow.
    if let Some((key_bytes, all_bytes)) = p.keys_bytes {
        let narrow_rows =
            whole.rows.saturating_mul(100) <= p.rows_total.saturating_mul(NARROW_PERCENT);
        let narrow_bytes = all_bytes > 0
            && key_bytes.saturating_mul(100) <= all_bytes.saturating_mul(NARROW_PERCENT);
        if narrow_rows && narrow_bytes {
            out.push(lint(
                "wide_before_narrow",
                p.key.clone(),
                format!(
                    "the key columns alone select {} of the twin's {} rows and hold {} of its {} \
                     bytes: a dedup or filter over every column reads about {}x what the key \
                     columns need; project the key columns first, select, then join the wide \
                     columns back",
                    thousands(whole.rows),
                    thousands(p.rows_total),
                    thousands(key_bytes),
                    thousands(all_bytes),
                    thousands(all_bytes / key_bytes.max(1))
                ),
                fresh,
                [
                    "select over every column".into(),
                    "project the key columns first, select, then join the wide columns back".into(),
                    "projection commutes with a selection on the projected columns".into(),
                ],
            ));
        }
    }
    // 4. dedup-before-filter.
    if latest_per_key
        && p.files_total > 0
        && whole.files.saturating_mul(100) < p.files_total.saturating_mul(SELECTIVE_FILES_PERCENT)
    {
        out.push(lint(
            "dedup_before_filter",
            p.key.clone(),
            format!(
                "the key sample is in {} of the twin's {} files (under {SELECTIVE_FILES_PERCENT}%), \
                 and the lookup asks for the latest row per key: taking the latest over the whole \
                 table first reads every file; filter by the keys first, then take the latest",
                thousands(whole.files),
                thousands(p.files_total)
            ),
            fresh,
            [
                "take the latest per key over the whole table, then filter by the keys".into(),
                "filter by the keys first, then take the latest per key".into(),
                "the newest version per sampled key is unchanged".into(),
            ],
        ));
    }
    out
}

/// The header line of a twin report.
pub(crate) fn note(of: &TwinOf) -> String {
    format!(
        "diagnosis on the lake twin of {}: every count is exact for the lake copy, and the \
         engine stores its tables differently, so its own costs are measured by fix, never \
         derived from these",
        of.engine
    )
}

impl Engine {
    /// The engine root `twin_of` names, as given (credentials included).
    fn twin_root(&self) -> Result<&str> {
        self.options()
            .twin_of
            .as_deref()
            .ok_or_else(|| Error::InvalidArgument {
                field: "twin_of",
                reason: "this needs the query engine the folder copies; name it with --twin-of \
                     <clickhouse://... | kusto://...>"
                    .into(),
            })
    }

    /// The engine adapter for `twin_of`, with `scratch` as the local folder
    /// exports land in.
    pub(crate) fn twin_adapter(&self, scratch: Option<&Path>) -> Result<Box<dyn EngineAdapter>> {
        let uri = self.twin_root()?;
        match &self.options().engine_adapter {
            Some(c) => (c.0)(uri, scratch),
            None => bellwether_source_engine::connect(uri, scratch),
        }
    }

    /// The engine's identity now.
    pub(crate) fn twin_identity(engine: &dyn EngineAdapter) -> Result<EngineIdentity> {
        bellwether_source_engine::identity(engine)
    }
}

/// The twin Evidence's engine side without a workload.
pub(crate) fn twin_of(
    engine: &dyn EngineAdapter,
    identity: &EngineIdentity,
    sequence: u64,
    lake_rows: u64,
    gates: &[Gate],
) -> TwinOf {
    TwinOf {
        engine: engine.root().to_string(),
        identity: identity.hash,
        units: identity.units,
        engine_rows: identity.rows,
        sequence,
        lake_rows,
        workload: None,
        keys: 0,
        predicted_rows: 0,
        predicted_row_groups: 0,
        predicted_files: 0,
        gates: gates.to_vec(),
    }
}

/// The prediction part of a twin from a pass.
pub(crate) fn predict(of: &mut TwinOf, p: &TwinPass, workload: Hash) {
    if let Some(w) = p.sets.get(&p.key) {
        of.workload = Some(workload);
        of.keys = w.keyset;
        of.predicted_rows = w.rows;
        of.predicted_row_groups = w.row_groups;
        of.predicted_files = w.files;
    }
}

/// What `workload record --from-engine` recorded (ADR 033).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineWorkload {
    /// The engine the keys came from (credentials removed).
    pub engine: String,
    /// The tag the keys query ran under (its query id in the engine's log).
    pub tag: String,
    /// The recorded workload: pass it to `why --workload`.
    pub workload: Hash,
    /// The key columns, in tuple order.
    pub columns: Vec<String>,
    /// Rows the keys query returned.
    pub rows: u64,
    /// Distinct non-null key tuples recorded.
    pub keys: u64,
    /// Rows dropped because their tuple held a null (ADR 014).
    pub null_tuples: u64,
}

impl Engine {
    /// Runs `keys_query` in the engine `twin_of` names, read-only, once,
    /// that run exporting its result as Parquet into a fresh folder of the
    /// scratch folder (governed as `fix`'s; the system's temporary folder
    /// when none is configured, for an engine that exports through its
    /// client), each created with the lake root's permissions, and records
    /// the result's `columns` (all of them when `None`) as a one-run
    /// workload over the data root's types. The export is deleted
    /// afterwards. A diagnosis then starts from the keys the engine itself
    /// produced, never a hand-built file; the tag reported is the one the
    /// engine logged the run under.
    pub fn record_from_engine(
        &self,
        keys_query: &str,
        columns: Option<&[String]>,
    ) -> Result<EngineWorkload> {
        crate::engine::guard(|| {
            let named = bellwether_source_engine::redact(self.twin_root()?);
            let run = bellwether_source_engine::Tag::run();
            let name = format!("bellwether-keys-{run}");
            let dir = if let Some(s) = &self.options().scratch {
                self.run_folder(&named, s, &name)?
            } else {
                let dir = std::env::temp_dir().join(name);
                self.private_dir(&named, &dir)?;
                dir
            };
            let recorded = (|| {
                let base = dir.parent().map(Path::to_path_buf).unwrap_or_default();
                let engine = self.twin_adapter(Some(&base))?;
                let tag = bellwether_source_engine::Tag::new(&run, "keys")?;
                let exported = engine
                    .export(keys_query, &tag, &dir)?
                    .exported
                    .unwrap_or_else(|| bellwether_source_engine::Exported {
                        files: Vec::new(),
                        bytes: 0,
                        rows: None,
                    });
                let mut batches = Vec::new();
                for f in &exported.files {
                    let kf = crate::keys::KeyFile {
                        path: f.clone(),
                        columns: columns.map(<[String]>::to_vec),
                        key_values: None,
                    };
                    batches.push(kf.read()?);
                }
                let batch = match batches.first() {
                    Some(b) => {
                        arrow::compute::concat_batches(&b.schema(), &batches).map_err(|e| {
                            Error::Other(format!("cannot join the exported keys ({e})"))
                        })?
                    }
                    None => {
                        return Err(Error::InvalidArgument {
                            field: "keys",
                            reason: format!(
                                "the keys query wrote no file in {}; it returned no rows, so \
                                 there are no keys to diagnose",
                                engine.root()
                            ),
                        })
                    }
                };
                let pinned = self.pin()?;
                let source = self.source(pinned.as_ref())?;
                let replayer = match &pinned {
                    Some(p) => bellwether_replay::Replayer::for_snapshot(&*source, &p.snapshot)?,
                    None => bellwether_replay::Replayer::unindexed(
                        &*source,
                        self.root(),
                        &self.partitioning(None)?,
                    )?,
                };
                let cache = self.cache()?;
                let mut recorder = bellwether_replay::WorkloadRecorder::new(
                    &cache,
                    std::sync::Arc::clone(replayer.schema()),
                );
                let rows = batch.num_rows() as u64;
                let cols: Vec<String> = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                recorder.record_query(
                    "keys",
                    bellwether_replay::KeyInput::Batch(&batch),
                    bellwether_replay::RunQuery::Locate,
                    None,
                )?;
                let w = recorder.finish()?;
                let first = w.runs().first().cloned();
                Ok(EngineWorkload {
                    engine: engine.root().to_string(),
                    tag: tag.to_string(),
                    workload: w.hash(),
                    columns: cols,
                    rows,
                    keys: w.keys_p50_p95_max()[2],
                    null_tuples: first.map_or(0, |r| r.null_tuples),
                })
            })();
            let _ = std::fs::remove_dir_all(&dir);
            recorded
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::assert_is_empty)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_are_nearest_rank() {
        assert_eq!(quantiles(vec![]), [0, 0, 0]);
        assert_eq!(quantiles(vec![5]), [5, 5, 5]);
        assert_eq!(quantiles((1..=100).collect()), [50, 95, 100]);
        assert_eq!(quantiles(vec![3, 1, 2]), [2, 3, 3]);
    }

    #[test]
    fn column_sets_cover_whole_prefixes_singles_and_small_subsets() {
        let s = column_sets(3);
        assert_eq!(s[0], vec![0, 1, 2]);
        assert_eq!(s.len(), 7);
        let wide = column_sets(6);
        assert!(wide.contains(&vec![0, 1]) && wide.contains(&vec![5]));
        assert!(!wide.contains(&vec![1, 3]));
    }

    /// A measured column set: `p50` rows per key, `keyset` keys covering
    /// `rows` rows in `files` files.
    fn set(columns: &[&str], p50: u64, keyset: u64, rows: u64, files: u64) -> TwinColumns {
        TwinColumns {
            columns: columns.iter().map(|c| (*c).to_string()).collect(),
            keyset,
            rows_per_key: [p50, p50, p50],
            files_per_key: [1, 1, 1],
            row_groups_per_key: [1, 1, 1],
            rows,
            row_groups: files,
            files,
            gates: Vec::new(),
        }
    }

    /// A pass over a sample keyed (batch, id): batch covers `batch_p50`
    /// rows per value and id `id_p50`; the whole key `keyset` keys in
    /// `files` of 1,000 files.
    fn pass_of(batch_p50: u64, id_p50: u64, keyset: u64, files: u64) -> TwinPass {
        let key = vec!["batch".to_string(), "id".to_string()];
        let mut sets = BTreeMap::new();
        for s in [
            set(&["batch"], batch_p50, 1, batch_p50, 500),
            set(&["id"], id_p50, keyset, id_p50, 500),
            set(&["batch", "id"], 1, keyset, keyset, files),
        ] {
            sets.insert(s.columns.clone(), s);
        }
        TwinPass {
            sets,
            key,
            row_groups_read: 0,
            rows_read: 0,
            keys_bytes: None,
            files_total: 1_000,
            rows_total: 100_000_000,
        }
    }

    fn fired(p: &TwinPass, gates: &[Gate], latest: bool) -> Vec<String> {
        lints(p, gates, latest, true)
            .into_iter()
            .map(|l| l.rule)
            .collect()
    }

    #[test]
    fn the_low_cardinality_lint_fires_above_its_rows_and_needs_a_partner_100_times_finer() {
        let low = |batch, id| fired(&pass_of(batch, id, 1, 500), &[], false);
        // At the threshold: not low-cardinality.
        assert!(low(LOW_CARDINALITY_ROWS, 1).is_empty());
        assert_eq!(low(LOW_CARDINALITY_ROWS + 1, 1), ["low_cardinality_key"]);
        // The partner must cover CARDINALITY_RATIO times fewer rows.
        let high = 200_000;
        assert_eq!(low(high, high / CARDINALITY_RATIO), ["low_cardinality_key"]);
        assert!(low(high, high / CARDINALITY_RATIO + 1).is_empty());
    }

    #[test]
    fn the_membership_lint_fires_above_the_gate_only() {
        let gate = [Gate {
            name: MEMBERSHIP_GATE.into(),
            limit: 10,
        }];
        assert!(fired(&pass_of(1, 1, 10, 500), &gate, false).is_empty());
        assert_eq!(
            fired(&pass_of(1, 1, 11, 500), &gate, false),
            ["unbounded_membership"]
        );
    }

    #[test]
    fn the_dedup_lint_fires_under_its_share_of_files_and_for_a_latest_lookup_only() {
        // 1% of 1,000 files is 10: at the share, not selective.
        assert!(fired(&pass_of(1, 1, 1, 10), &[], true).is_empty());
        assert_eq!(
            fired(&pass_of(1, 1, 1, 9), &[], true),
            ["dedup_before_filter"]
        );
        assert!(fired(&pass_of(1, 1, 1, 9), &[], false).is_empty());
    }

    #[test]
    fn a_candidate_s_columns_are_measured_in_any_order_of_the_sample() {
        let p = pass_of(1, 1, 3, 9);
        let swapped = measured(&p, &["id".to_string(), "batch".to_string()]).unwrap();
        assert_eq!(swapped.columns, ["id", "batch"]);
        assert_eq!(swapped.keyset, 3);
        assert!(measured(&p, &["id".to_string(), "day".to_string()]).is_none());
    }
}
