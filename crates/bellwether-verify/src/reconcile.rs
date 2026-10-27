//! The reconciler: hash-partitioned passes over both sides.
//!
//! Each pass `p` of `P` reads every row group in both sides' read sets,
//! encodes each row's key, and keeps only rows whose key hashes to `p`
//! (`xxh3_64(key, PARTITION_SEED) mod P`). For those it keeps per-key state
//! for both sides: rows seen, the newest version value, the count and
//! wrapping sum of the compared rows' row hashes, and, when the version rule
//! and totals are both set, the compared rows' totals by group. At the end
//! of the pass every key is classified and the state is dropped. Every
//! per-key decision depends only on that key's rows, and every total is an
//! exact, order-independent sum, so the result does not depend on `P`.

use std::mem::size_of;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{i256, DataType};
use bellwether_core::encoding;
use bellwether_core::render::render_tuple;
use bellwether_types::{
    format, AggServed, Coverage, Evidence, EvidenceBody, FileFingerprint, Hash, Provenance, Record,
    RowGroupMeta, ScanPlan, Source, TotalSpec,
};
use xxhash_rust::xxh3::{xxh3_128, xxh3_64_with_seed};

use crate::error::{Result, VerifyError};
use crate::outcome::{
    AggNotUsed, Counts, GroupRepr, GroupTotal, KeyRepr, MemoryReport, Mismatch, Reconciliation,
    SidePath, SideReport, Sides, VersionDigest,
};
use crate::planner::{AggTotals, ManifestIdentity, SidePlanner};
use crate::rowhash::{check_probe, hex, push_hashed_column};
use crate::schema::{class, Plan, TotalCol};
use crate::spec::{CoverageRequest, Options, ReconcileSpec, MAX_PASSES};
use crate::state::{
    charge_heap, heap, release_heap, table_peak, Contrib, Examples, GroupError, Groups, KeyState,
    Meter, MeteredMap, Overflow, SideState,
};
use crate::totals::{finalize, quantize, ExactTotal, Wide};

/// Seed of the key-space partition hash. Fixed: the partition never
/// depends on the caller's sampling seed.
pub const PARTITION_SEED: u64 = 0x6265_6c6c_7765_7468;

/// Assumed encoded width of a variable-width key element (string or
/// binary) when choosing the pass count. A wrong guess costs a restart,
/// never the bound.
const ASSUMED_VARLEN_KEY_BYTES: u64 = 32;

const SIDE_NAMES: [&str; 2] = ["left", "right"];

/// One side of a reconcile: a source, the root it reads, and optionally the
/// index over it.
#[derive(Clone, Copy)]
pub struct Side<'a> {
    source: &'a dyn Source,
    root: &'a str,
    planner: Option<&'a dyn SidePlanner>,
}

impl<'a> Side<'a> {
    /// An unindexed side: every row group is scanned.
    #[must_use]
    pub fn new(source: &'a dyn Source, root: &'a str) -> Self {
        Self {
            source,
            root,
            planner: None,
        }
    }

    /// The same side, indexed by `planner`: the certificate is anchored to
    /// its manifest, and workload coverage reads only planned row groups.
    #[must_use]
    pub fn indexed(mut self, planner: &'a dyn SidePlanner) -> Self {
        self.planner = Some(planner);
        self
    }

    /// Whether the side carries an index.
    #[must_use]
    pub fn is_indexed(&self) -> bool {
        self.planner.is_some()
    }

    pub(crate) fn planner(&self) -> Option<&'a dyn SidePlanner> {
        self.planner
    }
}

impl std::fmt::Debug for Side<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Side")
            .field("root", &self.root)
            .field("indexed", &self.planner.is_some())
            .finish_non_exhaustive()
    }
}

/// Reconciles two sides under one spec.
#[derive(Clone, Debug)]
pub struct Reconciler {
    spec: ReconcileSpec,
    options: Options,
    /// The `created_at` of every record this reconciler builds, when the
    /// caller owns the clock ([`Reconciler::created_at`]).
    created_at: Option<String>,
}

/// What the key scope is, resolved.
enum Scope {
    Full,
    Workload {
        workload: Hash,
        age: Duration,
        keys: Vec<Vec<u8>>,
        /// When the keys are values of an expression
        /// ([`crate::ReconcileSpec::keys_of`]), how a row's value is
        /// computed, and which keys some row held.
        expr: Option<ExprScope>,
    },
    /// The rows the row filter keeps, named after a workload
    /// ([`CoverageRequest::WorkloadRows`]).
    WorkloadRows {
        workload: Hash,
        age: Duration,
    },
    Sampled {
        requested: f64,
        seed: u64,
        threshold: Option<u64>,
    },
}

/// A workload scope whose keys are values of an expression.
struct ExprScope {
    typed: bellwether_core::expr::Typed,
    /// The expression's input columns: name and projection index.
    cols: Vec<(String, usize)>,
    /// Indices into the scope's sorted keys that some row (on either side)
    /// evaluated to; at most one entry per workload key.
    found: std::sync::Mutex<std::collections::BTreeSet<usize>>,
}

impl ExprScope {
    /// Whether row `row` of `cols` (projected) is in scope: its value is
    /// one of `keys`. Records which key it was.
    fn includes(
        &self,
        keys: &[Vec<u8>],
        cols: &[&dyn Array],
        row: usize,
    ) -> bellwether_types::Result<bool> {
        let named: Vec<(&str, &dyn Array)> = self
            .cols
            .iter()
            .map(|(c, i)| (c.as_str(), cols[*i]))
            .collect();
        let Some(v) = self.typed.encode_row(&named, row)? else {
            return Ok(false);
        };
        match keys.binary_search(&v) {
            Ok(i) => {
                self.found
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(i);
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    /// How many workload keys some row evaluated to.
    fn found(&self) -> u64 {
        self.found
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len() as u64
    }
}

impl Scope {
    fn includes(&self, key: &[u8]) -> bool {
        match self {
            Self::Full | Self::WorkloadRows { .. } => true,
            Self::Workload { keys, .. } => keys.binary_search_by(|k| k.as_slice().cmp(key)).is_ok(),
            Self::Sampled {
                seed, threshold, ..
            } => threshold.is_none_or(|t| xxh3_64_with_seed(key, *seed) < t),
        }
    }
}

/// The row groups a side reads in every pass.
enum ReadSet {
    Scan(Vec<FileFingerprint>),
    Planned(Vec<(FileFingerprint, Vec<u32>)>),
}

/// An indexed side's anchor and, for planned reads, its plan record.
struct Indexed {
    manifest: ManifestIdentity,
    /// The side's catalog digest, from the manifest and the source's reports.
    catalog: Hash,
    taken: Instant,
    plan: Option<(Evidence, Hash)>,
}

/// Everything resolved before the first pass.
struct Prepared<'s> {
    sides: [Side<'s>; 2],
    plan: Plan,
    scope: Scope,
    /// An upper bound on distinct keys from statistics, when one exists.
    key_bound: Option<u64>,
    reports: [SideReport; 2],
    read: [ReadSet; 2],
    indexed: [Option<Indexed>; 2],
    /// Per side, the `agg` key whose per-key rows and totals stand in for
    /// its rows (B7), when one does.
    agg: [Option<AggSide<'s>>; 2],
    /// The sides holding an agg key that could not stand in, and why.
    agg_not_used: Vec<AggNotUsed>,
}

/// A side whose per-key rows and totals an index's `agg` key answers (B7).
struct AggSide<'s> {
    totals: Box<dyn AggTotals + 's>,
    /// The agg key's identity.
    spec: String,
    /// The rows it aggregates: every row of the side.
    rows: u64,
}

/// What a side's index offers a reconcile (B7, [`agg_side`]).
enum AggUse<'s> {
    /// No complete agg key on the key columns storing a total's sum.
    Absent,
    /// One that stands in for the side's rows.
    Served(AggSide<'s>),
    /// One that cannot, and why.
    NotUsed(AggNotUsed),
}

/// A provenance input: its hash, its age when taken, and when it was taken.
pub(crate) type Input = (Hash, Duration, Instant);

/// A finished run, with what the certificate needs beyond the outcome.
pub(crate) struct Execution {
    pub outcome: Reconciliation,
    /// Provenance inputs, in citation order.
    pub inputs: Vec<Input>,
    /// Each side's manifest sequence, when it is indexed.
    pub manifests: [Option<u64>; 2],
    /// `plan` Evidence of the planned sides.
    pub evidence: Vec<Evidence>,
    /// The total columns and their quantization, in spec order
    /// (`CertBody::totals`).
    pub totals: Vec<TotalSpec>,
}

/// Why a pass stopped early.
enum Abort {
    Overflow,
    Error(VerifyError),
}

impl From<VerifyError> for Abort {
    fn from(e: VerifyError) -> Self {
        Self::Error(e)
    }
}

impl From<Overflow> for Abort {
    fn from(_: Overflow) -> Self {
        Self::Overflow
    }
}

impl From<bellwether_types::Error> for Abort {
    fn from(e: bellwether_types::Error) -> Self {
        Self::Error(e.into())
    }
}

/// A per-key difference held raw until the run ends.
#[derive(Clone, Copy)]
enum ExampleKind {
    MissingOnLeft,
    MissingOnRight,
    Differs,
    NoVersion(Sides),
}

#[derive(Clone, Copy)]
struct Digest {
    rows: u64,
    newest: Option<i64>,
    compared: u64,
    sum: u128,
}

impl Digest {
    fn of(s: &SideState) -> Self {
        Self {
            rows: s.rows,
            newest: s.newest,
            compared: s.compared,
            sum: s.sum,
        }
    }

    fn public(self) -> VersionDigest {
        VersionDigest {
            rows: self.rows,
            newest: self.newest,
            compared_rows: self.compared,
            hash: format!("{:032x}", self.sum),
        }
    }
}

struct Example {
    kind: ExampleKind,
    left: Digest,
    right: Digest,
}

/// The state of one attempt (a full run at one pass count).
struct Attempt {
    meter: Meter,
    groups: Option<Groups>,
    examples: Examples<Example>,
    counts: Counts,
    seen: u64,
    unsampled: u64,
    rows_read: [u64; 2],
    /// Rows read per side whose leading key column is null, counted in
    /// the first pass.
    null_keys: [u64; 2],
    peak_batch: u64,
}

/// One pass's per-key state.
struct PassState {
    keys: MeteredMap<KeyState>,
    unsampled: MeteredMap<()>,
}

/// Reusable per-row buffers.
#[derive(Default)]
struct Scratch {
    key: Vec<u8>,
    row: Vec<u8>,
    decimal: Vec<u8>,
    group: Vec<u8>,
    values: Vec<Option<i256>>,
    filter: Vec<u8>,
}

impl Reconciler {
    /// Validates `spec` (everything that does not need a schema) and builds
    /// a reconciler.
    pub fn new(spec: ReconcileSpec, options: Options) -> Result<Self> {
        spec.validate()?;
        if options.memory_budget == 0 {
            return Err(VerifyError::Budget("the memory budget is zero".into()));
        }
        Ok(Self {
            spec,
            options,
            created_at: None,
        })
    }

    /// Stamps every record this reconciler builds (the certificate and any
    /// `plan` Evidence) with `created_at` instead of this crate's clock, so
    /// a caller that writes several records in one operation gives them one
    /// clock and one format. It must be RFC 3339; the ledger refuses
    /// anything else.
    #[must_use]
    pub fn created_at(mut self, created_at: impl Into<String>) -> Self {
        self.created_at = Some(created_at.into());
        self
    }

    /// The time to stamp a record with: the caller's, else now.
    pub(crate) fn stamp(&self) -> Result<String> {
        match &self.created_at {
            Some(c) => Ok(c.clone()),
            None => now_rfc3339(),
        }
    }

    /// The spec.
    #[must_use]
    pub fn spec(&self) -> &ReconcileSpec {
        &self.spec
    }

    /// Compares `left` and `right` and returns the typed result. Writes
    /// nothing.
    pub fn run(&self, left: &Side<'_>, right: &Side<'_>) -> Result<Reconciliation> {
        Ok(self.execute(left, right)?.outcome)
    }

    pub(crate) fn execute(&self, left: &Side<'_>, right: &Side<'_>) -> Result<Execution> {
        let started = Instant::now();
        let prep = self.prepare([*left, *right])?;
        let mut passes = self.estimate_passes(&prep);
        let mut memory = MemoryReport {
            budget_bytes: self.options.memory_budget,
            ..MemoryReport::default()
        };
        let attempt = loop {
            let mut a = Attempt {
                meter: Meter::new(self.options.memory_budget),
                groups: (!prep.plan.totals.is_empty())
                    .then(|| Groups::new(prep.plan.totals.len(), self.options.memory_budget / 2)),
                examples: Examples::new(self.options.max_examples),
                counts: Counts::default(),
                seen: 0,
                unsampled: 0,
                rows_read: [0, 0],
                null_keys: [0, 0],
                peak_batch: 0,
            };
            let result = attempt(&prep, passes, &mut a);
            memory.peak_state_bytes = memory.peak_state_bytes.max(a.meter.peak());
            memory.peak_batch_bytes = memory.peak_batch_bytes.max(a.peak_batch);
            for (total, read) in memory.rows_read.iter_mut().zip(a.rows_read) {
                *total += read;
            }
            match result {
                Ok(()) => break a,
                Err(Abort::Error(e)) => return Err(e),
                Err(Abort::Overflow) => {
                    memory.restarts += 1;
                    passes = passes.saturating_mul(2);
                    if passes > MAX_PASSES {
                        return Err(VerifyError::Budget(format!(
                            "the comparison state does not fit in {} bytes even at {MAX_PASSES} \
                             passes; raise the memory budget (`--memory-budget` on the CLI, \
                             `Options::memory_budget` here)",
                            self.options.memory_budget
                        )));
                    }
                }
            }
        };
        memory.passes = passes;
        self.finish(prep, attempt, memory, started)
    }

    /// Discovery, schemas, scope, and read sets.
    #[allow(clippy::too_many_lines)] // per side: read set, catalog, agg key, report
    fn prepare<'s>(&self, sides: [Side<'s>; 2]) -> Result<Prepared<'s>> {
        let (left_files, left_index) = discover(&sides[0], 0)?;
        let (right_files, right_index) = discover(&sides[1], 1)?;
        let fps = [left_files, right_files];
        let mut indexed = [left_index, right_index];
        let schemas = [
            sides[0].source.schema(sides[0].root)?,
            sides[1].source.schema(sides[1].root)?,
        ];
        let plan = Plan::resolve(&self.spec, &schemas[0], &schemas[1])?;
        let scope = resolve_scope(&self.spec, &plan)?;
        let mut read: Vec<ReadSet> = Vec::with_capacity(2);
        let mut reports: Vec<SideReport> = Vec::with_capacity(2);
        let mut ranges: Vec<Option<(i128, i128)>> = Vec::with_capacity(2);
        let mut agg: Vec<Option<AggSide<'s>>> = Vec::with_capacity(2);
        let mut agg_not_used = Vec::new();
        for s in 0..2 {
            let (set, path) = read_set(
                &sides[s],
                s,
                &fps[s],
                &scope,
                &self.spec,
                indexed[s].as_mut(),
                &self.stamp()?,
            )?;
            let counted = count_rows(sides[s].source, &set, integer_key(&plan, &self.spec))?;
            let (mut row_groups, rows) = (counted.row_groups, counted.rows);
            ranges.push(counted.key_range);
            // The catalog digest (docs/format.md): the manifest's for an
            // indexed side, else the folder's at "0" (contract-v3 #29).
            let catalog = match &indexed[s] {
                Some(ix) => ix.catalog,
                None => bellwether_source::catalog::unindexed(
                    &schema_identity(&schemas[s]),
                    &counted.per_file,
                )?,
            };
            let used = agg_side(
                &self.spec,
                &plan,
                &scope,
                &sides[s],
                s,
                indexed[s].as_ref(),
                &fps[s],
            )?;
            let served = match used {
                AggUse::Served(a) => Some(a),
                AggUse::NotUsed(n) => {
                    agg_not_used.push(n);
                    None
                }
                AggUse::Absent => None,
            };
            let path = match &served {
                Some(a) => {
                    // Nothing is read: the plan a workload scope made is
                    // not cited.
                    if let Some(ix) = indexed[s].as_mut() {
                        ix.plan = None;
                    }
                    row_groups = 0;
                    SidePath::Agg {
                        spec: a.spec.clone(),
                    }
                }
                None => path,
            };
            agg.push(served);
            reports.push(SideReport {
                root: sides[s].root.to_owned(),
                files: fps[s].len() as u64,
                catalog,
                manifest: indexed[s]
                    .as_ref()
                    .map(|ix| (ix.manifest.hash, ix.manifest.sequence)),
                path,
                row_groups,
                rows,
                null_key_rows: 0,
            });
            read.push(set);
        }
        // Distinct keys of a single integer key are at most the union of
        // the two sides' value ranges, plus one for a null key.
        let key_bound = match (ranges[0], ranges[1]) {
            (Some((a, b)), Some((c, d))) => {
                let span = b.max(d) - a.min(c) + 2;
                u64::try_from(span).ok()
            }
            _ => None,
        };
        Ok(Prepared {
            sides,
            plan,
            scope,
            key_bound,
            reports: reports
                .try_into()
                .unwrap_or_else(|_| unreachable!("two sides")),
            read: read
                .try_into()
                .unwrap_or_else(|_| unreachable!("two sides")),
            indexed,
            agg: agg.try_into().unwrap_or_else(|_| unreachable!("two sides")),
            agg_not_used,
        })
    }

    /// The pass count the statistics call for: the worst-case accounted
    /// state of the keys a pass may hold, over the budget left after a
    /// reserve for the group totals and examples.
    fn estimate_passes(&self, prep: &Prepared<'_>) -> u32 {
        // Distinct keys per pass: at most the rows of the larger side when
        // the sides share their keys (if they do not, a pass overflows and
        // the run restarts), at most the integer key's value range, and at
        // most the workload's keys.
        let rows = prep.reports[0].rows.max(prep.reports[1].rows);
        let mut entries = prep.key_bound.map_or(rows, |b| rows.min(b));
        if let Scope::Workload {
            keys, expr: None, ..
        } = &prep.scope
        {
            entries = entries.min(keys.len() as u64);
        }
        let key_len: u64 = prep.plan.key_types(0).iter().map(encoded_width).sum();
        let key_heap = heap(usize::try_from(key_len).unwrap_or(usize::MAX));
        // Heap each kept key owns beside its slot: the key, and with both a
        // version rule and totals, one group contribution per side.
        let mut per_key_heap = key_heap;
        if prep.plan.latest_by.is_some() && !prep.plan.totals.is_empty() {
            let c = Contrib {
                entries: vec![(0, 0, vec![Wide::default(); prep.plan.totals.len()].into())],
            };
            per_key_heap += 2 * c.heap_bytes();
        }
        let slot = size_of::<(Box<[u8]>, KeyState)>();
        let set_slot = size_of::<(Box<[u8]>, ())>();
        let sampled = match prep.scope {
            Scope::Sampled { requested, .. } => Some(requested),
            _ => None,
        };
        // Kept for the whole run: the examples, and a modest allowance for
        // group totals (a group table that outgrows it makes a pass
        // overflow and the run restart; past half the budget it is refused).
        let example = heap(usize::try_from(key_len).unwrap_or(usize::MAX))
            + 2 * size_of::<(Box<[u8]>, Example)>() as u64;
        // Examples are charged only for real mismatches, so their reserve
        // is capped; more than that makes a pass overflow and restart.
        let mut reserve = example
            .saturating_mul(entries.min(self.options.max_examples as u64))
            .min(self.options.memory_budget / 8);
        if !prep.plan.totals.is_empty() {
            reserve += self.options.memory_budget / 16;
        }
        let available = self.options.memory_budget.saturating_sub(reserve).max(1);
        // The meter's own model of one pass holding `n` keys: the table's
        // growth (largest resize or final size) plus the keys' heap.
        let need = |n: u64| -> u64 {
            match sampled {
                None => table_peak(n, slot) + n * per_key_heap,
                Some(f) => {
                    #[allow(
                        clippy::cast_precision_loss,
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss
                    )]
                    let kept = (n as f64 * f).ceil() as u64;
                    let rest = n.saturating_sub(kept);
                    table_peak(kept, slot)
                        + kept * per_key_heap
                        + table_peak(rest, set_slot)
                        + rest * key_heap
                }
            }
        };
        for passes in 1..=MAX_PASSES {
            let n = entries.div_ceil(u64::from(passes));
            // Hash partitions are uneven: allow 5% plus three standard
            // deviations over the mean.
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let spread = 3 * (n as f64).sqrt().ceil() as u64;
            let n = n + n / 20 + spread;
            if need(n) <= available {
                return passes;
            }
        }
        MAX_PASSES
    }

    /// Totals, examples, coverage, and the outcome.
    fn finish(
        &self,
        prep: Prepared<'_>,
        mut a: Attempt,
        memory: MemoryReport,
        started: Instant,
    ) -> Result<Execution> {
        let examples = a.examples.into_sorted(&mut a.meter);
        let mut mismatches = render_examples(examples, &prep.plan.key_types(0));
        let mut totals = Vec::new();
        if let Some(groups) = a.groups.take() {
            let result = self.compare_totals(&prep.plan, &groups, &mut a.counts, &mut mismatches);
            groups.free(&mut a.meter);
            totals = result?;
        }
        nothing_compared(&prep.scope, a.seen, a.unsampled, prep.plan.filter.is_some())?;
        let coverage = realized_coverage(&prep.scope, &mut a.counts, a.seen, a.unsampled);
        let c = &a.counts;
        let pass = c.missing_left == 0
            && c.missing_right == 0
            && c.mismatched == 0
            && c.no_comparable_version == 0
            && c.totals_differ == 0;
        debug_assert_eq!(a.meter.used(), 0, "every charge is returned");
        let (inputs, manifests, evidence) = provenance(&prep, started);
        let agg = prep
            .agg
            .iter()
            .enumerate()
            .filter_map(|(s, a)| {
                a.as_ref().map(|a| AggServed {
                    side: SIDE_NAMES[s].to_string(),
                    specs: vec![a.spec.clone()],
                    rows: a.rows,
                    checked: None,
                })
            })
            .collect();
        let mut sides = prep.reports;
        for (report, n) in sides.iter_mut().zip(a.null_keys) {
            report.null_key_rows = n;
        }
        Ok(Execution {
            outcome: Reconciliation {
                pass,
                coverage,
                counts: a.counts,
                mismatches,
                totals,
                key_columns: self.spec.key_columns.clone(),
                latest_by: self.spec.latest_by.clone(),
                row_filter: self.spec.row_filter.clone(),
                hashed_columns: prep.plan.hashed_names.clone(),
                ignored_columns: prep.plan.ignored.clone(),
                sides,
                memory,
                agg,
                agg_not_used: prep.agg_not_used,
            },
            inputs,
            manifests,
            evidence,
            totals: self
                .spec
                .total_columns
                .iter()
                .map(|t| TotalSpec {
                    column: t.column.clone(),
                    quantize: t.quantize,
                })
                .collect(),
        })
    }

    /// Every group's exact totals on both sides; a differing one is a
    /// mismatch. Groups come in canonical order, columns in spec order.
    fn compare_totals(
        &self,
        plan: &Plan,
        groups: &Groups,
        counts: &mut Counts,
        mismatches: &mut Vec<Mismatch>,
    ) -> Result<Vec<GroupTotal>> {
        let group_types = plan.group_types(0);
        let mut totals = Vec::new();
        for (gkey, acc) in groups.sorted() {
            if acc.rows == [0, 0] {
                // Interned by a version that a newer one superseded.
                continue;
            }
            let rendered = render_tuple(gkey, &group_types).unwrap_or_default();
            let repr = GroupRepr {
                canonical: hex(gkey),
                values: self
                    .spec
                    .group_by
                    .iter()
                    .cloned()
                    .zip(rendered.into_iter().chain(std::iter::repeat(None)))
                    .collect(),
            };
            for (c, t) in plan.totals.iter().enumerate() {
                let fin = |side: usize| -> Result<ExactTotal> {
                    finalize(acc.sums[side][c], t.wide, t.scale).ok_or_else(|| {
                        VerifyError::Overflow {
                            column: t.name.clone(),
                            width: if t.wide { "i256" } else { "i128" },
                            group: if repr.values.is_empty() {
                                String::new()
                            } else {
                                format!(" in group {}", repr.label(&t.name))
                            },
                        }
                    })
                };
                let (left, right) = (fin(0)?, fin(1)?);
                if left != right {
                    counts.totals_differ += 1;
                    mismatches.push(Mismatch::TotalDiffers {
                        group: repr.clone(),
                        column: t.name.clone(),
                        left,
                        right,
                    });
                }
                totals.push(GroupTotal {
                    group: repr.clone(),
                    column: t.name.clone(),
                    left_rows: acc.rows[0],
                    right_rows: acc.rows[1],
                    left,
                    right,
                });
            }
        }
        Ok(totals)
    }
}

/// Lists a side's files sorted by path and, for an indexed side, checks
/// that its manifest tracks exactly those files.
fn discover(side: &Side<'_>, s: usize) -> Result<(Vec<FileFingerprint>, Option<Indexed>)> {
    let mut found = side.source.discover(side.root)?;
    found.sort_by(|a, b| a.path.cmp(&b.path));
    if let Some(w) = found.windows(2).find(|w| w[0].path == w[1].path) {
        return Err(VerifyError::SourceContract(format!(
            "{} side discovered {:?} twice",
            SIDE_NAMES[s], w[0].path
        )));
    }
    let Some(planner) = side.planner else {
        return Ok((found, None));
    };
    let mut manifest = planner.manifest()?;
    let taken = Instant::now();
    if manifest.rowgroup_ids.len() != manifest.files.len()
        || manifest.chunks.len() != manifest.files.len()
    {
        return Err(VerifyError::Index(format!(
            "the {} side's manifest lists {} files but {} row-group id lists and {} chunk lists",
            SIDE_NAMES[s],
            manifest.files.len(),
            manifest.rowgroup_ids.len(),
            manifest.chunks.len()
        )));
    }
    // Manifest order is the catalog's; the stale check compares by path.
    let in_manifest_order: Vec<Tracked> = manifest
        .files
        .iter()
        .cloned()
        .zip(manifest.rowgroup_ids.iter().cloned())
        .zip(manifest.chunks.iter().cloned())
        .map(|((f, ids), c)| (f, ids, c))
        .collect();
    let mut paired: Vec<(FileFingerprint, Vec<u32>)> = in_manifest_order
        .iter()
        .map(|(f, ids, _)| (f.clone(), ids.clone()))
        .collect();
    paired.sort_by(|a, b| a.0.path.cmp(&b.0.path));
    (manifest.files, manifest.rowgroup_ids) = paired.into_iter().unzip();
    if manifest.files != found {
        return Err(VerifyError::Index(format!(
            "the {} side's manifest (sequence {}) tracks {} files and the source discovers {}, \
             or their fingerprints differ; rebuild the index before reconciling against it",
            SIDE_NAMES[s],
            manifest.sequence,
            manifest.files.len(),
            found.len()
        )));
    }
    // The side's own report of every tracked file, which must have as many
    // row groups as the manifest assigned (a text source at another chunk
    // size would not), makes its catalog digest with the manifest's ids.
    let mut reports = Vec::with_capacity(in_manifest_order.len());
    for (fp, ids, chunks) in in_manifest_order {
        let rgs = side.source.row_groups(&fp)?;
        // Every tracked row group must cover the byte range the manifest
        // recorded (text sources); a shifted chunking can keep the count
        // and the lengths and still name other rows.
        let recorded: Vec<Option<(u64, u64)>> = match &chunks {
            Some(c) => c.iter().copied().map(Some).collect(),
            None => vec![None; ids.len()],
        };
        if let Some((i, rg)) = rgs
            .iter()
            .enumerate()
            .find(|(i, rg)| recorded.get(*i).copied().flatten() != rg.byte_range)
        {
            return Err(VerifyError::Index(format!(
                "the {} side's source reads row group {i} of {:?} as bytes {:?} where its index \
                 recorded {:?}; a text source must use the chunk size the manifest records",
                SIDE_NAMES[s],
                fp.path,
                rg.byte_range,
                recorded.get(i).copied().flatten()
            )));
        }
        if rgs.len() != ids.len() {
            return Err(VerifyError::Index(format!(
                "the {} side's source reports {} row groups for {:?} and its index {}; a text \
                 source must use the chunk size the manifest records",
                SIDE_NAMES[s],
                rgs.len(),
                fp.path,
                ids.len()
            )));
        }
        reports.push((fp, ids.into_iter().zip(rgs).collect::<Vec<_>>()));
    }
    let catalog = bellwether_source::catalog::indexed(
        manifest.sequence,
        &schema_identity(&manifest.dataset_schema),
        &reports,
        &manifest.tombstoned,
    )?;
    Ok((
        found,
        Some(Indexed {
            manifest,
            catalog,
            taken,
            plan: None,
        }),
    ))
}

/// A tracked file in manifest order: its fingerprint, assigned ids, and
/// recorded byte ranges.
type Tracked = (FileFingerprint, Vec<u32>, Option<Vec<(u64, u64)>>);

/// The dataset schema as the catalog digest covers it, with core's
/// recorded type spelling.
fn schema_identity(
    schema: &arrow::datatypes::Schema,
) -> Vec<bellwether_source::catalog::SchemaColumn> {
    bellwether_source::catalog::schema_identity(schema, |t| {
        bellwether_core::segment::ColumnType::of(t)
            .ok()
            .and_then(|c| serde_json::to_string(&c).ok())
    })
}

/// Resolves the coverage request against the key types.
fn resolve_scope(spec: &ReconcileSpec, plan: &Plan) -> Result<Scope> {
    Ok(match &spec.coverage {
        CoverageRequest::Full => Scope::Full,
        CoverageRequest::Workload(w) => {
            let expr = match &spec.keys_of {
                Some(text) => Some(expr_scope(text, plan)?),
                None => None,
            };
            if let Some(e) = &expr {
                for k in &w.keys {
                    check_probe(k, std::slice::from_ref(e.typed.data_type()))?;
                }
            } else {
                let types = plan.key_types(0);
                for k in &w.keys {
                    check_probe(k, &types)?;
                }
            }
            let mut keys = w.keys.clone();
            keys.sort_unstable();
            keys.dedup();
            Scope::Workload {
                workload: w.workload,
                age: w.age,
                keys,
                expr,
            }
        }
        CoverageRequest::Sampled { fraction, seed } => Scope::Sampled {
            requested: *fraction,
            seed: *seed,
            threshold: (*fraction < 1.0).then(|| {
                // floor(fraction × 2^64); the product is below 2^64.
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let t = (*fraction * 18_446_744_073_709_551_616.0) as u64;
                t
            }),
        },
        CoverageRequest::WorkloadRows { workload, age } => Scope::WorkloadRows {
            workload: *workload,
            age: *age,
        },
        CoverageRequest::StaticProof { identity } => {
            return Err(VerifyError::StaticProofUnsupported {
                identity: identity.clone(),
            })
        }
    })
}

/// The expression a workload's keys are values of, checked against the
/// projected columns at the left side's types.
fn expr_scope(text: &str, plan: &Plan) -> Result<ExprScope> {
    let parsed = bellwether_core::expr::Expr::parse(text)
        .map_err(|e| VerifyError::InvalidSpec(e.to_string()))?;
    let mut cols = Vec::new();
    let mut fields = Vec::new();
    for c in parsed.columns() {
        let i = plan
            .projection
            .iter()
            .position(|p| *p == c)
            .ok_or_else(|| {
                VerifyError::Schema(format!(
                    "the workload expression {text} reads column {c:?}, which is not compared"
                ))
            })?;
        fields.push(arrow::datatypes::Field::new(
            &c,
            plan.types[0][i].clone(),
            true,
        ));
        cols.push((c, i));
    }
    let typed = bellwether_core::expr::Typed::new(parsed, &arrow::datatypes::Schema::new(fields))
        .map_err(|e| VerifyError::InvalidSpec(e.to_string()))?;
    Ok(ExprScope {
        typed,
        cols,
        found: std::sync::Mutex::new(std::collections::BTreeSet::new()),
    })
}

/// A side's read set: the planned row groups for workload coverage on an
/// indexed side, otherwise every row group of every discovered file.
fn read_set(
    side: &Side<'_>,
    s: usize,
    files: &[FileFingerprint],
    scope: &Scope,
    spec: &ReconcileSpec,
    indexed: Option<&mut Indexed>,
    created_at: &str,
) -> Result<(ReadSet, SidePath)> {
    let (
        Scope::Workload {
            keys, expr: None, ..
        },
        Some(ix),
        Some(planner),
    ) = (scope, indexed, side.planner)
    else {
        return Ok((ReadSet::Scan(files.to_vec()), SidePath::Scan));
    };
    let side_schema = side.source.schema(side.root)?;
    check_index_key_types(spec, s, &ix.manifest, &side_schema)?;
    let plan = planner.plan_keys(&spec.key_columns, keys)?;
    let selected = resolve_plan(side.source, files, &plan, &ix.manifest, s)?;
    let path_counts = (plan.row_groups_selected, plan.row_groups_total);
    let evidence = plan_evidence(plan, &ix.manifest, ix.taken, created_at);
    let hash = Record::Evidence(evidence.clone()).hash()?;
    ix.plan = Some((evidence, hash));
    Ok((
        ReadSet::Planned(selected),
        SidePath::Planned {
            manifest_sequence: ix.manifest.sequence,
            plan_evidence: hash,
            row_groups_selected: path_counts.0,
            row_groups_total: path_counts.1,
        },
    ))
}

/// The index must encode every key column exactly as the side does.
fn check_index_key_types(
    spec: &ReconcileSpec,
    s: usize,
    manifest: &ManifestIdentity,
    side_schema: &arrow::datatypes::Schema,
) -> Result<()> {
    for c in &spec.key_columns {
        let mine = side_schema
            .field_with_name(c)
            .map_err(|e| VerifyError::Schema(e.to_string()))?
            .data_type();
        let theirs = manifest
            .dataset_schema
            .field_with_name(c)
            .map_err(|_| {
                VerifyError::Index(format!(
                    "the {} side's index has no column {c:?} in its dataset schema",
                    SIDE_NAMES[s]
                ))
            })?
            .data_type();
        if class(mine)? != class(theirs)? {
            return Err(VerifyError::Index(format!(
                "key column {c:?} is {mine} in the {} side's source and {theirs} in its index; \
                 a probe would name another value there",
                SIDE_NAMES[s]
            )));
        }
    }
    Ok(())
}

/// The key column whose statistics bound the distinct keys: a single key
/// column of an integer type.
fn integer_key<'p>(plan: &Plan, spec: &'p ReconcileSpec) -> Option<&'p str> {
    let [only] = spec.key_columns.as_slice() else {
        return None;
    };
    let ints = plan
        .key_types(0)
        .iter()
        .chain(plan.key_types(1).iter())
        .all(|t| {
            matches!(
                class(t),
                Ok(crate::schema::Class::Int | crate::schema::Class::UInt)
            )
        });
    ints.then_some(only.as_str())
}

/// Encoded bytes of one key element of type `t`, for the pass estimate.
fn encoded_width(t: &DataType) -> u64 {
    match t {
        DataType::Boolean => 2,
        DataType::Decimal128(..) => 18,
        DataType::Decimal256(..) => 34,
        DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(..) => 11,
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Dictionary(..) => ASSUMED_VARLEN_KEY_BYTES,
        _ => 9,
    }
}

/// One complete run at `passes` passes.
fn attempt(prep: &Prepared<'_>, passes: u32, a: &mut Attempt) -> std::result::Result<(), Abort> {
    let mut scratch = Scratch {
        values: vec![None; prep.plan.totals.len()],
        ..Scratch::default()
    };
    for p in 0..passes {
        let mut pass = PassState {
            keys: MeteredMap::new(),
            unsampled: MeteredMap::new(),
        };
        let outcome = run_pass(prep, passes, p, &mut pass, a, &mut scratch);
        let seen = pass.keys.len() as u64;
        let unsampled = pass.unsampled.len() as u64;
        let classified = outcome.and_then(|()| classify(prep, &pass.keys, a));
        pass.keys.free(&mut a.meter);
        pass.unsampled.free(&mut a.meter);
        classified?;
        a.seen += seen;
        a.unsampled += unsampled;
    }
    Ok(())
}

/// Folds the per-key rows and totals of an `agg` key (B7) into partition
/// `p` as if its side's rows had been read: each key's `n` rows add `n` to
/// its rows and compared rows and `n` copies of its one row hash (every
/// hashed column is a key column, so every row of the key hashes alike),
/// and its sum and rows to its group's totals.
fn fold_agg_side(
    prep: &Prepared<'_>,
    s: usize,
    agg: &AggSide<'_>,
    (passes, p): (u32, u32),
    pass: &mut PassState,
    a: &mut Attempt,
) -> std::result::Result<(), Abort> {
    let plan = &prep.plan;
    let mut stop: Option<Abort> = None;
    let mut row = Vec::new();
    let visited = agg.totals.visit(&mut |key, rows, sum| {
        if passes > 1 && xxh3_64_with_seed(key, PARTITION_SEED) % u64::from(passes) != u64::from(p)
        {
            return Ok(());
        }
        let outcome = (|| -> std::result::Result<(), Abort> {
            if !prep.scope.includes(key) {
                if matches!(prep.scope, Scope::Sampled { .. })
                    && !pass.unsampled.map.contains_key(key)
                {
                    pass.unsampled.insert_new(&mut a.meter, key, (), 0)?;
                }
                return Ok(());
            }
            let hash = agg_row_hash(plan, key, &mut row).ok_or_else(|| {
                Abort::Error(VerifyError::Index(format!(
                    "the {} side's agg key {} holds a key that is not a tuple of the key \
                     columns; rebuild it",
                    SIDE_NAMES[s], agg.spec
                )))
            })?;
            if !pass.keys.map.contains_key(key) {
                pass.keys
                    .insert_new(&mut a.meter, key, KeyState::default(), 0)?;
            }
            let state = pass
                .keys
                .map
                .get_mut(key)
                .unwrap_or_else(|| unreachable!("inserted above"));
            let side = &mut state.sides[s];
            side.rows += rows;
            side.compared += rows;
            side.sum = side.sum.wrapping_add(hash.wrapping_mul(u128::from(rows)));
            if let Some(groups) = a.groups.as_mut() {
                // The group is the key itself, or the one group.
                let group: &[u8] = if plan.group.is_empty() { &[] } else { key };
                let gid = intern(groups, &mut a.meter, group)?;
                let acc = groups.acc_mut(gid);
                acc.rows[s] += rows;
                if let Some(total) = acc.sums[s].first_mut() {
                    total.add(i256::from_i128(sum));
                }
            }
            Ok(())
        })();
        outcome.map_err(|e| {
            stop = Some(e);
            bellwether_types::Error::Other("stopped".into())
        })
    });
    if let Some(e) = stop {
        return Err(e);
    }
    visited?;
    Ok(())
}

/// The row hash of every row of `key` when every hashed column is a key
/// column: each hashed column's name and the key's element for it.
fn agg_row_hash(plan: &Plan, key: &[u8], row: &mut Vec<u8>) -> Option<u128> {
    let elements = encoding::tuple_elements(key)?;
    row.clear();
    for name in &plan.hashed_names {
        let i = plan.key.iter().position(|&k| plan.projection[k] == *name)?;
        let len = u32::try_from(name.len()).ok()?;
        row.extend_from_slice(&len.to_be_bytes());
        row.extend_from_slice(name.as_bytes());
        row.extend_from_slice(&key[elements.get(i)?.clone()]);
    }
    Some(xxh3_128(row))
}

/// What a side's index offers a reconcile (B7): [`AggUse::Absent`] unless
/// the side is indexed and its index holds a complete agg key on exactly
/// the key columns storing the sum of one of the totals. That key stands in
/// for the side's rows ([`AggUse::Served`]) when no version rule, row
/// filter, or expression scope selects among a key's rows; every hashed
/// column is a key column; the one total is a Decimal128 column summed as
/// stored, grouped by nothing or by exactly the key; the side's types are
/// the index's; and its rows are every row of the side (a row whose
/// leading key column is null is in no agg key, ADR 014). Otherwise the
/// side is read and [`AggUse::NotUsed`] says why, and what to change when
/// a change of the request would let it answer.
#[allow(clippy::too_many_lines)] // one cause per condition
fn agg_side<'s>(
    spec: &ReconcileSpec,
    plan: &Plan,
    scope: &Scope,
    side: &Side<'s>,
    s: usize,
    ix: Option<&Indexed>,
    files: &[FileFingerprint],
) -> Result<AggUse<'s>> {
    let (Some(planner), Some(ix)) = (side.planner(), ix) else {
        return Ok(AggUse::Absent);
    };
    let mut found = None;
    for t in &plan.totals {
        let column = &plan.projection[t.proj];
        if let Some(totals) = planner.agg_totals(&spec.key_columns, column)? {
            found = Some((t, column, totals));
            break;
        }
    }
    let Some((t, column, totals)) = found else {
        return Ok(AggUse::Absent);
    };
    let mut causes = Vec::new();
    let mut next = None;
    let mut compared_at = None;
    match scope {
        Scope::Full | Scope::Sampled { .. } | Scope::Workload { expr: None, .. } => {}
        Scope::Workload { expr: Some(_), .. } => causes.push(format!(
            "the keys are values of the expression {}, not of the key columns",
            spec.keys_of.as_deref().unwrap_or_default()
        )),
        Scope::WorkloadRows { .. } => causes.push(
            "only the rows a workload's runs read are compared, and the agg key totals every row"
                .to_string(),
        ),
    }
    if let Some(c) = &spec.latest_by {
        causes.push(format!(
            "the newest row per key by {c} is compared, and the agg key totals every row"
        ));
    }
    if plan.filter.is_some() && !matches!(scope, Scope::WorkloadRows { .. }) {
        causes.push(
            "a row filter keeps only some of each key's rows, and the agg key totals every row"
                .to_string(),
        );
    }
    if !(spec.group_by.is_empty() || spec.group_by == spec.key_columns) {
        causes.push(format!(
            "totals are grouped by {}, and the agg key totals per key",
            spec.group_by.join(", ")
        ));
    }
    if plan.totals.len() > 1 {
        let names: Vec<&str> = plan
            .totals
            .iter()
            .map(|t| plan.projection[t.proj].as_str())
            .collect();
        causes.push(format!(
            "{} are totalled, and an agg key stands in for one total",
            names.join(", ")
        ));
    }
    if t.quantize {
        causes.push(format!(
            "{column} is totalled quantized, and an agg key's sum of floats is never exact"
        ));
    } else if t.wide {
        causes.push(format!(
            "{column} is a Decimal256, whose totals an agg key cannot hold"
        ));
    }
    let compared: Vec<&String> = plan
        .hashed_names
        .iter()
        .filter(|c| !spec.key_columns.contains(c))
        .collect();
    if !compared.is_empty() {
        let list = compared
            .iter()
            .map(|c| c.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        causes.push(format!(
            "{list} {} compared row by row, and the agg key holds only each key's rows and the \
             total of {column}",
            if compared.len() == 1 { "is" } else { "are" }
        ));
        compared_at = Some(causes.len() - 1);
        let mut ignore: Vec<&str> = plan.ignored.iter().map(String::as_str).collect();
        ignore.extend(compared.iter().map(|c| c.as_str()));
        ignore.sort_unstable();
        ignore.dedup();
        let what = if compared.as_slice() == [column] {
            format!("compares {column} only through its exact total")
        } else {
            format!("compares each key's rows and only the exact total of {column}")
        };
        next = Some(format!(
            "ignore {list} too (--ignore {} on the CLI, `ignore` here): the reconcile then \
             {what}, which the agg key answers",
            ignore.join(",")
        ));
    }
    let in_index = |name: &str| {
        ix.manifest
            .dataset_schema
            .field_with_name(name)
            .ok()
            .map(|f| f.data_type().clone())
    };
    let side_schema = side.source.schema(side.root)?;
    for c in std::iter::once(column).chain(&spec.key_columns) {
        let mine = side_schema
            .field_with_name(c)
            .ok()
            .map(arrow::datatypes::Field::data_type);
        let theirs = in_index(c);
        if mine != theirs.as_ref() {
            causes.push(format!(
                "{c} is {} on this side and {} in its index",
                mine.map_or_else(|| "absent".to_string(), ToString::to_string),
                theirs.map_or_else(|| "absent".to_string(), |t| t.to_string()),
            ));
        }
    }
    let not_used = |causes: Vec<String>, next: Option<String>| {
        AggUse::NotUsed(AggNotUsed {
            side: SIDE_NAMES[s].to_string(),
            spec: totals.spec(),
            causes,
            next,
        })
    };
    // The `--ignore` step is printed only when the compared columns are the
    // one cause left; otherwise it names the causes that would remain, so
    // following it never leaves the side still read.
    let compared_cause = compared_at;
    let remaining = |causes: &[String]| -> String {
        let rest: Vec<&str> = causes
            .iter()
            .enumerate()
            .filter(|(i, _)| Some(*i) != compared_cause)
            .map(|(_, c)| c.as_str())
            .collect();
        format!(
            "ignoring {} too would not let the agg key answer: {}",
            compared
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            rest.join("; ")
        )
    };
    let finish = |causes: Vec<String>, next: Option<String>| {
        let next = match next {
            Some(_) if causes.len() > 1 => Some(remaining(&causes)),
            n => n,
        };
        not_used(causes, next)
    };
    if causes.len() > usize::from(compared_cause.is_some()) {
        return Ok(finish(causes, next));
    }
    let all = count_rows(side.source, &ReadSet::Scan(files.to_vec()), None)?.rows;
    let mut rows = 0u64;
    let visited = totals.visit(&mut |_, n, _| {
        rows += n;
        Ok(())
    });
    match visited {
        Ok(()) => {}
        Err(e) if bellwether_core::agg::is_sum_overflow(&e) => {
            causes.push(format!(
                "a key's total of {column} overflows the agg key's 128-bit sum, so the rows \
                 are read to total it exactly"
            ));
            return Ok(finish(causes, next));
        }
        Err(e) => return Err(e.into()),
    }
    if rows != all {
        let nulls = all.saturating_sub(rows);
        causes.push(format!(
            "{nulls} of its {all} rows {} a null {} and {} in no agg key (ADR 014), so the \
             rows are read to compare them",
            if nulls == 1 { "has" } else { "have" },
            spec.key_columns[0],
            if nulls == 1 { "is" } else { "are" },
        ));
        return Ok(finish(causes, next));
    }
    if !causes.is_empty() {
        return Ok(finish(causes, next));
    }
    Ok(AggUse::Served(AggSide {
        spec: totals.spec(),
        totals,
        rows,
    }))
}

/// Reads every row group of both sides once, keeping partition `p`.
fn run_pass(
    prep: &Prepared<'_>,
    passes: u32,
    p: u32,
    pass: &mut PassState,
    a: &mut Attempt,
    scratch: &mut Scratch,
) -> std::result::Result<(), Abort> {
    for s in 0..2 {
        if let Some(agg) = &prep.agg[s] {
            fold_agg_side(prep, s, agg, (passes, p), pass, a)?;
            continue;
        }
        let source = prep.sides[s].source;
        match &prep.read[s] {
            ReadSet::Scan(files) => {
                for fp in files {
                    for rg in source.row_groups(fp)? {
                        read_row_group(prep, s, &rg, (passes, p), pass, a, scratch)?;
                    }
                }
            }
            ReadSet::Planned(files) => {
                for (fp, indices) in files {
                    let rgs = source.row_groups(fp)?;
                    for &i in indices {
                        let rg = rgs.get(i as usize).ok_or_else(|| {
                            VerifyError::Index(format!(
                                "{}: planned row group {i} no longer exists",
                                fp.path
                            ))
                        })?;
                        read_row_group(prep, s, rg, (passes, p), pass, a, scratch)?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Reads one row group's projected columns and folds the rows of partition
/// `p` into the pass state.
#[allow(clippy::too_many_lines)]
fn read_row_group(
    prep: &Prepared<'_>,
    s: usize,
    rg: &RowGroupMeta,
    (passes, p): (u32, u32),
    pass: &mut PassState,
    a: &mut Attempt,
    scratch: &mut Scratch,
) -> std::result::Result<(), Abort> {
    let plan = &prep.plan;
    let batch = prep.sides[s].source.read_columns(rg, &plan.projection)?;
    check_batch(&batch, rg, &plan.projection, s)?;
    a.rows_read[s] += batch.num_rows() as u64;
    a.peak_batch = a.peak_batch.max(batch.get_array_memory_size() as u64);
    let cols: Vec<&dyn Array> = batch.columns().iter().map(AsRef::as_ref).collect();
    let types = &plan.types[s];
    let locate = |column: usize, e: &bellwether_types::Error| VerifyError::Unrepresentable {
        side: SIDE_NAMES[s],
        file: rg.file.path.clone(),
        row_group: rg.index_in_file,
        column: plan.projection[column].clone(),
        detail: e.to_string(),
    };
    for row in 0..batch.num_rows() {
        let key = &mut scratch.key;
        key.clear();
        for &i in &plan.key {
            let start = key.len();
            encoding::encode_nullable_as(cols[i], row, &types[i], key)
                .map_err(|e| locate(i, &e))?;
            // A null element is the lone marker; checked before the
            // partition so the outcome does not depend on the pass count.
            if plan.latest_by.is_some() && key[start..] == [encoding::tag::NULL] {
                return Err(Abort::Error(VerifyError::NullKey {
                    side: SIDE_NAMES[s],
                    file: rg.file.path.clone(),
                    row_group: rg.index_in_file,
                    column: plan.projection[i].clone(),
                }));
            }
        }
        // A null leading key column is the lone marker first (ADR 014):
        // counted once, in the first pass.
        if p == 0 && key.first() == Some(&encoding::tag::NULL) {
            a.null_keys[s] += 1;
        }
        if passes > 1 && xxh3_64_with_seed(key, PARTITION_SEED) % u64::from(passes) != u64::from(p)
        {
            continue;
        }
        let in_scope = match &prep.scope {
            Scope::Workload {
                keys,
                expr: Some(e),
                ..
            } => e.includes(keys, &cols, row).map_err(|e| {
                Abort::Error(VerifyError::Unrepresentable {
                    side: SIDE_NAMES[s],
                    file: rg.file.path.clone(),
                    row_group: rg.index_in_file,
                    column: "(workload expression)".into(),
                    detail: e.to_string(),
                })
            })?,
            scope => scope.includes(key),
        };
        if !in_scope {
            if matches!(prep.scope, Scope::Sampled { .. })
                && !pass.unsampled.map.contains_key(key.as_slice())
            {
                pass.unsampled.insert_new(&mut a.meter, key, (), 0)?;
            }
            continue;
        }
        if !filter_keeps(plan, &cols, s, rg, row, &mut scratch.filter)? {
            continue;
        }
        let ts = match plan.latest_by {
            Some(i) => encoding::ts_value_as(cols[i], row, &types[i]).map_err(|e| locate(i, &e))?,
            None => None,
        };
        scratch.row.clear();
        for (&i, name) in plan.hashed.iter().zip(&plan.hashed_names) {
            push_hashed_column(&mut scratch.row, name, cols[i], row, &types[i])
                .map_err(|e| locate(i, &e))?;
        }
        let hash = xxh3_128(&scratch.row);
        let gid = match a.groups.as_mut() {
            Some(groups) => {
                for (slot, t) in scratch.values.iter_mut().zip(&plan.totals) {
                    *slot = if t.quantize {
                        quantized_at(cols[t.proj], row, t).map_err(|(value, why)| {
                            Abort::Error(VerifyError::Quantize {
                                side: SIDE_NAMES[s],
                                file: rg.file.path.clone(),
                                row_group: rg.index_in_file,
                                total: t.name.clone(),
                                key: key_label(&scratch.key, plan, s),
                                value: format!("{value} in {:?} ({why})", plan.projection[t.proj]),
                            })
                        })?
                    } else {
                        decimal_at(cols[t.proj], row, &types[t.proj], &mut scratch.decimal)
                            .map_err(|e| locate(t.proj, &e))?
                    };
                }
                scratch.group.clear();
                for &i in &plan.group {
                    encoding::encode_nullable_as(cols[i], row, &types[i], &mut scratch.group)
                        .map_err(|e| locate(i, &e))?;
                }
                Some(intern(groups, &mut a.meter, &scratch.group)?)
            }
            None => None,
        };
        let row_state = Row {
            side: s,
            ts,
            hash,
            gid,
            values: &scratch.values,
        };
        fold_row(
            plan.latest_by.is_some(),
            &mut pass.keys,
            a,
            &scratch.key,
            &row_state,
        )?;
    }
    Ok(())
}

/// Whether the row filter keeps row `row` (every row without one): some
/// residual keeps it ([`crate::ReconcileSpec::row_filter`]).
fn filter_keeps(
    plan: &Plan,
    cols: &[&dyn Array],
    s: usize,
    rg: &RowGroupMeta,
    row: usize,
    buf: &mut Vec<u8>,
) -> std::result::Result<bool, VerifyError> {
    let Some(tests) = &plan.filter else {
        return Ok(true);
    };
    for t in tests {
        let kept =
            t.keeps(cols, &plan.types[s], row, buf)
                .map_err(|e| VerifyError::Unrepresentable {
                    side: SIDE_NAMES[s],
                    file: rg.file.path.clone(),
                    row_group: rg.index_in_file,
                    column: "(row filter)".into(),
                    detail: e.to_string(),
                })?;
        if kept {
            return Ok(true);
        }
    }
    Ok(false)
}

/// One in-scope row, encoded.
struct Row<'v> {
    side: usize,
    ts: Option<i64>,
    hash: u128,
    gid: Option<u32>,
    values: &'v [Option<i256>],
}

fn intern(groups: &mut Groups, meter: &mut Meter, group: &[u8]) -> std::result::Result<u32, Abort> {
    match groups.intern(meter, group) {
        Ok(id) => Ok(id),
        Err(GroupError::Overflow) => Err(Abort::Overflow),
        Err(GroupError::TooMany(n)) => Err(Abort::Error(VerifyError::Budget(format!(
            "group_by produced over {n} groups, whose totals exceed half of the memory budget; \
             raise the memory budget (`--memory-budget` on the CLI, `Options::memory_budget` here) or \
             group more coarsely"
        )))),
    }
}

/// Folds one row into its key's state on its side.
fn fold_row(
    latest: bool,
    keys: &mut MeteredMap<KeyState>,
    a: &mut Attempt,
    key: &[u8],
    row: &Row<'_>,
) -> std::result::Result<(), Abort> {
    if !keys.map.contains_key(key) {
        keys.insert_new(&mut a.meter, key, KeyState::default(), 0)?;
    }
    let state = keys
        .map
        .get_mut(key)
        .unwrap_or_else(|| unreachable!("inserted above"));
    let side = &mut state.sides[row.side];
    side.rows += 1;
    if !latest {
        // Every row is compared: rows add to the group totals directly.
        side.compared += 1;
        side.sum = side.sum.wrapping_add(row.hash);
        if let (Some(groups), Some(gid)) = (a.groups.as_mut(), row.gid) {
            let acc = groups.acc_mut(gid);
            acc.rows[row.side] += 1;
            for (sum, v) in acc.sums[row.side].iter_mut().zip(row.values) {
                if let Some(v) = v {
                    sum.add(*v);
                }
            }
        }
        return Ok(());
    }
    let Some(ts) = row.ts else {
        // A null version value never wins (ADR 014).
        return Ok(());
    };
    if side.newest.is_none_or(|n| ts > n) {
        side.newest = Some(ts);
        side.compared = 0;
        side.sum = 0;
        if let Some(old) = side.contrib.take() {
            release_heap(&mut keys.charged_heap, &mut a.meter, old.heap_bytes());
        }
    }
    if side.newest == Some(ts) {
        side.compared += 1;
        side.sum = side.sum.wrapping_add(row.hash);
        if let Some(gid) = row.gid {
            add_contrib(side, &mut keys.charged_heap, &mut a.meter, gid, row.values)?;
        }
    }
    Ok(())
}

/// Classifies every key of a finished pass and folds its totals.
fn classify(
    prep: &Prepared<'_>,
    keys: &MeteredMap<KeyState>,
    a: &mut Attempt,
) -> std::result::Result<(), Abort> {
    let latest = prep.plan.latest_by.is_some();
    for (key, st) in keys.iter() {
        let [l, r] = &st.sides;
        let (pl, pr) = (l.rows > 0, r.rows > 0);
        let (vl, vr) = (
            latest && pl && l.newest.is_none(),
            latest && pr && r.newest.is_none(),
        );
        let kind = if vl || vr {
            // Counted in `mismatched`, which the certificate carries, and
            // broken out in `no_comparable_version` (contract-v3 #26).
            a.counts.mismatched += 1;
            a.counts.no_comparable_version += 1;
            Some(ExampleKind::NoVersion(match (vl, vr) {
                (true, true) => Sides::Both,
                (true, false) => Sides::Left,
                _ => Sides::Right,
            }))
        } else if !pl {
            a.counts.missing_left += 1;
            Some(ExampleKind::MissingOnLeft)
        } else if !pr {
            a.counts.missing_right += 1;
            Some(ExampleKind::MissingOnRight)
        } else if l.compared == r.compared && l.sum == r.sum {
            a.counts.matched += 1;
            None
        } else {
            a.counts.mismatched += 1;
            Some(ExampleKind::Differs)
        };
        if let Some(kind) = kind {
            let example = Example {
                kind,
                left: Digest::of(l),
                right: Digest::of(r),
            };
            a.examples.offer(&mut a.meter, key, example)?;
        }
        if let Some(groups) = a.groups.as_mut() {
            for (s, side) in st.sides.iter().enumerate() {
                for (gid, rows, sums) in side.contrib.iter().flat_map(|c| &c.entries) {
                    let acc = groups.acc_mut(*gid);
                    acc.rows[s] += rows;
                    for (into, from) in acc.sums[s].iter_mut().zip(sums.iter()) {
                        into.merge(from);
                    }
                }
            }
        }
    }
    Ok(())
}

/// The recorded examples as typed mismatches, in canonical key order.
fn render_examples(examples: Vec<(Box<[u8]>, Example)>, key_types: &[DataType]) -> Vec<Mismatch> {
    examples
        .into_iter()
        .map(|(key, ex)| {
            let key = KeyRepr {
                canonical: hex(&key),
                values: render_tuple(&key, key_types).unwrap_or_default(),
            };
            let (left, right) = (ex.left.public(), ex.right.public());
            match ex.kind {
                ExampleKind::MissingOnLeft => Mismatch::MissingOnLeft { key, right },
                ExampleKind::MissingOnRight => Mismatch::MissingOnRight { key, left },
                ExampleKind::Differs => Mismatch::RowHashDiffers { key, left, right },
                ExampleKind::NoVersion(sides) => Mismatch::NoComparableVersion {
                    key,
                    sides,
                    left,
                    right,
                },
            }
        })
        .collect()
}

/// Refuses a scoped coverage that compared no key while there was data to
/// compare: a workload none of whose keys exists on either side, or a
/// sample that selected none of the keys present. With a row filter
/// (`filtered`), a key is compared only when a row of it is kept.
fn nothing_compared(scope: &Scope, seen: u64, unsampled: u64, filtered: bool) -> Result<()> {
    match scope {
        Scope::Workload { keys, .. } if seen == 0 && filtered => {
            Err(VerifyError::NothingCompared(format!(
                "none of the {} workload keys has a row the lookups ask for (the row filter keeps \
                 none) on either side, so there is nothing to verify",
                keys.len()
            )))
        }
        Scope::Workload { keys, .. } if seen == 0 => Err(VerifyError::NothingCompared(format!(
            "none of the {} workload keys is on either side; check that they are encoded at \
             the dataset types (decimal scale is not visible in the bytes)",
            keys.len()
        ))),
        Scope::WorkloadRows { .. } if seen == 0 => Err(VerifyError::NothingCompared(
            "no row on either side holds what the workload's lookups ask for (the row filter \
             keeps none), so there is nothing to verify; check the recorded values against the \
             data"
                .into(),
        )),
        Scope::Sampled { requested, .. } if seen == 0 && unsampled > 0 => {
            Err(VerifyError::NothingCompared(format!(
                "a sample at fraction {requested} selected none of the {unsampled} keys present; \
                 raise the fraction"
            )))
        }
        _ => Ok(()),
    }
}

/// The coverage that actually ran; fills `keys_compared` and
/// `absent_both`.
fn realized_coverage(scope: &Scope, counts: &mut Counts, seen: u64, unsampled: u64) -> Coverage {
    match scope {
        Scope::Full => {
            counts.keys_compared = seen;
            Coverage::Full
        }
        Scope::Workload {
            workload,
            keys,
            expr,
            ..
        } => {
            let supplied = keys.len() as u64;
            counts.keys_compared = supplied;
            // With an expression the rows' keys are not the workload's
            // values; a value is absent when no row on either side had it.
            let present = expr.as_ref().map_or(seen, ExprScope::found);
            counts.absent_both = supplied.saturating_sub(present);
            Coverage::Workload {
                workload: *workload,
                keys: supplied,
            }
        }
        Scope::WorkloadRows { workload, .. } => {
            counts.keys_compared = seen;
            Coverage::Workload {
                workload: *workload,
                keys: seen,
            }
        }
        Scope::Sampled { requested, .. } => {
            counts.keys_compared = seen;
            let total = seen + unsampled;
            // With no keys at all the realized fraction is 0/0; the request
            // is the only fraction there is.
            #[allow(clippy::cast_precision_loss)]
            let fraction = if total == 0 {
                *requested
            } else {
                seen as f64 / total as f64
            };
            Coverage::Sampled {
                keys: seen,
                fraction,
            }
        }
    }
}

/// The provenance inputs of a run, each indexed side's manifest sequence,
/// and the plan Evidence to write.
fn provenance(
    prep: &Prepared<'_>,
    started: Instant,
) -> (Vec<Input>, [Option<u64>; 2], Vec<Evidence>) {
    let mut inputs: Vec<Input> = prep
        .reports
        .iter()
        .map(|r| (r.catalog, Duration::ZERO, started))
        .collect();
    let mut manifests = [None; 2];
    let mut evidence = Vec::new();
    for (s, ix) in prep.indexed.iter().enumerate() {
        let Some(ix) = ix else { continue };
        inputs.push((ix.manifest.hash, ix.manifest.age, ix.taken));
        manifests[s] = Some(ix.manifest.sequence);
        if let Some((ev, h)) = &ix.plan {
            inputs.push((*h, ix.manifest.age, ix.taken));
            evidence.push(ev.clone());
        }
    }
    match &prep.scope {
        Scope::Workload { workload, age, .. } | Scope::WorkloadRows { workload, age } => {
            inputs.push((*workload, *age, started));
        }
        Scope::Full | Scope::Sampled { .. } => {}
    }
    (inputs, manifests, evidence)
}

/// Adds one compared row's totals to a key's per-group contributions,
/// charging any allocation before it happens.
fn add_contrib(
    side: &mut SideState,
    charged_heap: &mut u64,
    meter: &mut Meter,
    gid: u32,
    values: &[Option<i256>],
) -> std::result::Result<(), Overflow> {
    let exists = side
        .contrib
        .as_ref()
        .is_some_and(|c| c.entries.iter().any(|e| e.0 == gid));
    if !exists {
        let before = side.contrib.as_ref().map_or(0, |c| c.heap_bytes());
        // Worst case of a push: the box (if new), the vector reallocating
        // (old and new buffers both alive), and one sum per column.
        let entry = size_of::<(u32, u64, Box<[Wide]>)>();
        let (len, cap) = side
            .contrib
            .as_ref()
            .map_or((0, 0), |c| (c.entries.len(), c.entries.capacity()));
        // A key's compared rows usually fall in one group, so a new
        // contribution starts with room for exactly one entry.
        let grown = if side.contrib.is_none() {
            heap(size_of::<Contrib>()) + heap(entry)
        } else if len == cap {
            heap((2 * cap).max(4) * entry)
        } else {
            0
        };
        let worst = grown + heap(values.len() * size_of::<Wide>());
        charge_heap(charged_heap, meter, worst)?;
        let c = side.contrib.get_or_insert_with(|| {
            Box::new(Contrib {
                entries: Vec::with_capacity(1),
            })
        });
        c.entries.push((
            gid,
            0,
            vec![Wide::default(); values.len()].into_boxed_slice(),
        ));
        let added = c.heap_bytes() - before;
        debug_assert!(added <= worst, "contribution grew past its charge");
        release_heap(charged_heap, meter, worst.saturating_sub(added));
    }
    let c = side
        .contrib
        .as_mut()
        .unwrap_or_else(|| unreachable!("created above"));
    let e = c
        .entries
        .iter_mut()
        .find(|e| e.0 == gid)
        .unwrap_or_else(|| unreachable!("pushed above"));
    e.1 += 1;
    for (sum, v) in e.2.iter_mut().zip(values) {
        if let Some(v) = v {
            sum.add(*v);
        }
    }
    Ok(())
}

/// A decimal value at its dataset scale, through the canonical encoder so
/// the rescale (exact, or an error) is the one the row hash uses.
fn decimal_at(
    array: &dyn Array,
    row: usize,
    dataset_type: &DataType,
    scratch: &mut Vec<u8>,
) -> bellwether_types::Result<Option<i256>> {
    use encoding::width;
    scratch.clear();
    if !encoding::encode_array_value_as(array, row, dataset_type, scratch)? {
        return Ok(None);
    }
    let bad = || bellwether_types::Error::Other("not a decimal encoding".into());
    match scratch.get(1) {
        Some(&width::DECIMAL128) => {
            let mut raw: [u8; 16] = scratch
                .get(2..18)
                .ok_or_else(bad)?
                .try_into()
                .map_err(|_| bad())?;
            raw[0] ^= 0x80;
            Ok(Some(i256::from_i128(i128::from_be_bytes(raw))))
        }
        Some(&width::DECIMAL256) => {
            let mut raw: [u8; 32] = scratch
                .get(2..34)
                .ok_or_else(bad)?
                .try_into()
                .map_err(|_| bad())?;
            raw[0] ^= 0x80;
            Ok(Some(i256::from_be_bytes(raw)))
        }
        _ => Err(bad()),
    }
}

/// A float total's value quantized at its scale, or the value as printed
/// and why it cannot be totalled.
fn quantized_at(
    array: &dyn Array,
    row: usize,
    t: &TotalCol,
) -> std::result::Result<Option<i256>, (String, String)> {
    let v = match encoding::value_at(array, row) {
        Ok(None) => return Ok(None),
        Ok(Some(encoding::Value::Float(v))) => v,
        Ok(Some(_)) => return Err((String::new(), "not a float".into())),
        Err(e) => return Err((String::new(), e.to_string())),
    };
    let scale = u8::try_from(t.scale).map_err(|_| (v.to_string(), "negative scale".into()))?;
    quantize(v, scale)
        .map(|q| Some(i256::from_i128(q)))
        .map_err(|e| (v.to_string(), e.to_string()))
}

/// A key for messages: `(col=value, ...)` from its canonical bytes.
fn key_label(key: &[u8], plan: &Plan, side: usize) -> String {
    let names = plan.key.iter().map(|&i| plan.projection[i].as_str());
    let values = render_tuple(key, &plan.key_types(side)).unwrap_or_default();
    let parts: Vec<String> = names
        .zip(values.into_iter().chain(std::iter::repeat(None)))
        .map(|(n, v)| format!("{n}={}", v.as_deref().unwrap_or("null")))
        .collect();
    format!("({})", parts.join(", "))
}

/// The batch must be the requested columns of the whole row group.
fn check_batch(
    batch: &RecordBatch,
    rg: &RowGroupMeta,
    projection: &[String],
    s: usize,
) -> Result<()> {
    let names: Vec<&String> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.name())
        .collect();
    if names.len() != projection.len() || names.iter().zip(projection).any(|(a, b)| *a != b) {
        return Err(VerifyError::SourceContract(format!(
            "{} side, {} row group {}: asked for columns {projection:?}, got {names:?}",
            SIDE_NAMES[s], rg.file.path, rg.index_in_file
        )));
    }
    if batch.num_rows() as u64 != rg.rows {
        return Err(VerifyError::SourceContract(format!(
            "{} side, {} row group {}: statistics say {} rows, the read returned {}",
            SIDE_NAMES[s],
            rg.file.path,
            rg.index_in_file,
            rg.rows,
            batch.num_rows()
        )));
    }
    Ok(())
}

/// What a side's statistics say about its read set.
struct Counted {
    row_groups: u64,
    rows: u64,
    /// For a scan, each file with its row-group count, in read order.
    per_file: Vec<(FileFingerprint, Vec<RowGroupMeta>)>,
    /// The smallest and largest value of a single integer key column over
    /// every row group, when every row group reports both bounds.
    key_range: Option<(i128, i128)>,
}

/// Row groups, rows, and the integer key range in a read set, from the
/// sources' statistics.
fn count_rows(source: &dyn Source, set: &ReadSet, key: Option<&str>) -> Result<Counted> {
    let mut c = Counted {
        row_groups: 0,
        rows: 0,
        per_file: Vec::new(),
        key_range: None,
    };
    let mut per_file = Vec::new();
    let mut bounded = key.is_some();
    let mut fold = |rg: &RowGroupMeta| {
        c.row_groups += 1;
        c.rows += rg.rows;
        if !bounded || rg.rows == 0 {
            return;
        }
        let stats = rg.stats.iter().find(|st| Some(st.column.as_str()) == key);
        let bounds =
            stats.and_then(|st| Some((int_bound(st.min.as_ref()?)?, int_bound(st.max.as_ref()?)?)));
        if let Some((lo, hi)) = bounds {
            c.key_range = Some(
                c.key_range
                    .map_or((lo, hi), |(a, b)| (a.min(lo), b.max(hi))),
            );
        } else {
            bounded = false;
            c.key_range = None;
        }
    };
    match set {
        ReadSet::Scan(files) => {
            for fp in files {
                let rgs = source.row_groups(fp)?;
                for rg in &rgs {
                    fold(rg);
                }
                per_file.push((fp.clone(), rgs));
            }
        }
        ReadSet::Planned(files) => {
            for (fp, indices) in files {
                let rgs = source.row_groups(fp)?;
                for &i in indices {
                    if let Some(rg) = rgs.get(i as usize) {
                        fold(rg);
                    }
                }
            }
        }
    }
    c.per_file = per_file;
    Ok(c)
}

/// The value of a canonical integer bound (`0x02` signed or `0x03`
/// unsigned, 8 bytes).
fn int_bound(bytes: &[u8]) -> Option<i128> {
    let raw: [u8; 8] = bytes.get(1..9)?.try_into().ok()?;
    let v = u64::from_be_bytes(raw);
    match *bytes.first()? {
        encoding::tag::INT => {
            #[allow(clippy::cast_possible_wrap)]
            let signed = (v ^ (1 << 63)) as i64;
            Some(i128::from(signed))
        }
        encoding::tag::UINT => Some(i128::from(v)),
        _ => None,
    }
}

/// Resolves a plan against the files a side discovered.
fn resolve_plan(
    source: &dyn Source,
    discovered: &[FileFingerprint],
    plan: &ScanPlan,
    manifest: &ManifestIdentity,
    s: usize,
) -> Result<Vec<(FileFingerprint, Vec<u32>)>> {
    if plan.manifest_version != manifest.sequence.to_string() {
        return Err(VerifyError::Index(format!(
            "the {} side's plan was computed against manifest {:?}, not sequence {}",
            SIDE_NAMES[s], plan.manifest_version, manifest.sequence
        )));
    }
    let mut out: Vec<(FileFingerprint, Vec<u32>)> = Vec::with_capacity(plan.entries.len());
    for entry in &plan.entries {
        let known = discovered
            .binary_search_by(|f| f.path.cmp(&entry.file.path))
            .ok()
            .map(|i| &discovered[i]);
        if known != Some(&entry.file) {
            return Err(VerifyError::Index(format!(
                "the {} side's plan names {:?}, which the source did not discover with that \
                 fingerprint",
                SIDE_NAMES[s], entry.file.path
            )));
        }
        if out.iter().any(|(f, _)| f.path == entry.file.path) {
            return Err(VerifyError::Index(format!(
                "the {} side's plan lists {:?} twice",
                SIDE_NAMES[s], entry.file.path
            )));
        }
        let rgs = source.row_groups(&entry.file)?;
        let available = rgs.len();
        let indices: Vec<u32> = entry.row_groups.iter().collect();
        if let Some(bad) = indices.iter().find(|&&i| i as usize >= available) {
            return Err(VerifyError::Index(format!(
                "the {} side's plan selects row group {bad} of {:?}, which has {available}",
                SIDE_NAMES[s], entry.file.path
            )));
        }
        // The plan's bytes are the index's row-group sizes: they must be the
        // source's, or the indices name other byte ranges.
        let bytes: u64 = indices.iter().map(|&i| rgs[i as usize].bytes).sum();
        if bytes != entry.bytes {
            return Err(VerifyError::Index(format!(
                "the {} side's plan puts {} bytes in {:?} where its source has {bytes} for those \
                 row groups; a text source must use the chunk size the manifest records",
                SIDE_NAMES[s], entry.bytes, entry.file.path
            )));
        }
        out.push((entry.file.clone(), indices));
    }
    out.sort_by(|a, b| a.0.path.cmp(&b.0.path));
    Ok(out)
}

/// The `plan` Evidence that records which row groups a planned side read.
fn plan_evidence(
    plan: ScanPlan,
    m: &ManifestIdentity,
    taken: Instant,
    created_at: &str,
) -> Evidence {
    Evidence {
        record_version: format::RECORD_VERSION,
        provenance: Provenance {
            inputs: vec![m.hash],
            oldest_input_age: m.age + taken.elapsed(),
            manifest_version: Some(m.sequence.to_string()),
            created_at: created_at.to_string(),
        },
        body: EvidenceBody::Plan { plan },
    }
}

/// The current time, RFC 3339 UTC with millisecond precision: the format
/// every other record writer uses (`2026-10-24T21:03:02.069Z`).
pub(crate) fn now_rfc3339() -> Result<String> {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| VerifyError::Engine(bellwether_types::Error::Other(format!("clock: {e}"))))?;
    // `rfc3339_utc` prints nine fractional digits; keep three.
    let nanos = bellwether_source::storage::rfc3339_utc(since);
    Ok(format!("{}Z", &nanos[..nanos.len() - 7]))
}
