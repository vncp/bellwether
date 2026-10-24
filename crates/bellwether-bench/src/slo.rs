//! `--assert-slo`: budgets from ADR 018 and `docs/work-packages.md`.
//!
//! Every check reads per-run data in `results.json`, never the display
//! summary, and has three outcomes: pass, fail, or skip (nothing to compare,
//! for example matched rows when scans are off). A gate must never pass
//! vacuously, so:
//!
//! - an index budget that is set while `results.json` holds no index
//!   results fails, and so does a budget whose sample is empty;
//! - the build budget is asserted on build results, never lookup results;
//! - a latency budget fails when scans did not run;
//! - in strict mode (bare `--assert-slo`, what CI runs) a skip on a core
//!   check fails too: matched rows, bytes read, and RSS growth on Linux.
//!
//! Enforced now, for the baselines:
//!
//! - **`generator_rss_growth`** (ADR 018): process RSS growth over the whole
//!   generation stays within [`crate::generate::rss_growth_bound_bytes`].
//! - **`generator_buffered_bound`**: the generator's own buffers stay within
//!   [`crate::generate::memory_bound_bytes`].
//! - **`harness_completes`**: every run has a result from every available
//!   strategy, with no errors.
//! - **`no_false_negatives`** (invariant 1).
//! - **`plan_cost_reported`**: each plan's own byte and row-group counts
//!   equal the cost recomputed from the footers.
//! - **`same_rows`**: matched rows and the exact `amount` total equal the
//!   oracle's for every strategy that read rows, and plan strategies' row
//!   digests equal `raw_scan`'s.
//! - **`bytes_read_matches_plan`**: for every scanned plan, the bytes
//!   fetched from storage and the bytes handed to the decoder both equal the
//!   planned bytes from statistics (the realized-versus-predicted claim).
//! - **`baselines_present`**.
//!
//! Named and not yet enforced: the `INDEX_*` and `BUILD_*` constants are
//! `None` until the index lands. Setting one to `Some` turns its check on.

use std::fmt::Write as _;
use std::path::Path;

use crate::corpus::GenParams;
use crate::generate::{generate, memory_bound_bytes, rss_growth_bound_bytes, GeneratorStats};
use crate::harness::{run_all, RunOptions};
use crate::results::{
    CorpusResult, Results, RunResult, StrategyKind, StrategyResult, INDEX, PARQUET_STATS, RAW_SCAN,
};
use crate::stats::quantile_sorted;
use crate::truth::write_ground_truth;
use crate::workload::{generate_workload, WorkloadParams};
use crate::{BenchError, Result};

/// TODO(WP1/WP4): p95 milliseconds for one index lookup plan
/// (`plan_ms` of the `index` strategy). `None` until the index lands.
pub const INDEX_PLAN_P95_BUDGET_MS: Option<f64> = None;
/// TODO(WP1/WP4): p95 end-to-end milliseconds for one index lookup
/// (`latency_ms` of the `index` strategy); requires scanned runs.
pub const INDEX_LATENCY_P95_BUDGET_MS: Option<f64> = None;
/// TODO(WP1): bytes one lookup may hold (one block index plus the bitmaps for
/// the keys asked, ADR 018), checked against `peak_memory_bytes` of every
/// index result. A missing measurement fails.
pub const INDEX_LOOKUP_MEMORY_BOUND_BYTES: Option<u64> = None;
/// TODO(WP1/WP4): largest allowed ratio of index row groups to oracle row
/// groups per run (1.0 for an exact index; above 1.0 for bloom).
pub const INDEX_MAX_ROW_GROUPS_OVER_ORACLE: Option<f64> = None;
/// TODO(WP4): build streams one row group per worker (ADR 018); bytes per
/// worker, checked against `build[].peak_memory_per_worker_bytes`. With no
/// build results the check fails.
pub const BUILD_MEMORY_BOUND_PER_WORKER_BYTES: Option<u64> = None;
/// Observe-tier latency budget from `docs/architecture.md` (under 100 ms).
/// Asserted by WP12 against the MCP observe tools, not by this harness.
pub const OBSERVE_LATENCY_BUDGET_MS: f64 = 100.0;

/// The budgets a run is held to.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Budgets {
    /// See [`INDEX_PLAN_P95_BUDGET_MS`].
    pub index_plan_p95_ms: Option<f64>,
    /// See [`INDEX_LATENCY_P95_BUDGET_MS`].
    pub index_latency_p95_ms: Option<f64>,
    /// See [`INDEX_LOOKUP_MEMORY_BOUND_BYTES`].
    pub index_lookup_memory_bytes: Option<u64>,
    /// See [`INDEX_MAX_ROW_GROUPS_OVER_ORACLE`].
    pub index_max_row_groups_over_oracle: Option<f64>,
    /// See [`BUILD_MEMORY_BOUND_PER_WORKER_BYTES`].
    pub build_memory_per_worker_bytes: Option<u64>,
}

impl Budgets {
    /// The budgets in force: the constants above.
    pub const CURRENT: Self = Self {
        index_plan_p95_ms: INDEX_PLAN_P95_BUDGET_MS,
        index_latency_p95_ms: INDEX_LATENCY_P95_BUDGET_MS,
        index_lookup_memory_bytes: INDEX_LOOKUP_MEMORY_BOUND_BYTES,
        index_max_row_groups_over_oracle: INDEX_MAX_ROW_GROUPS_OVER_ORACLE,
        build_memory_per_worker_bytes: BUILD_MEMORY_BOUND_PER_WORKER_BYTES,
    };

    fn any_index(&self) -> bool {
        self.index_plan_p95_ms.is_some()
            || self.index_latency_p95_ms.is_some()
            || self.index_lookup_memory_bytes.is_some()
            || self.index_max_row_groups_over_oracle.is_some()
    }

    fn pending(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.index_plan_p95_ms.is_none() {
            out.push("INDEX_PLAN_P95_BUDGET_MS");
        }
        if self.index_latency_p95_ms.is_none() {
            out.push("INDEX_LATENCY_P95_BUDGET_MS");
        }
        if self.index_lookup_memory_bytes.is_none() {
            out.push("INDEX_LOOKUP_MEMORY_BOUND_BYTES");
        }
        if self.index_max_row_groups_over_oracle.is_none() {
            out.push("INDEX_MAX_ROW_GROUPS_OVER_ORACLE");
        }
        if self.build_memory_per_worker_bytes.is_none() {
            out.push("BUILD_MEMORY_BOUND_PER_WORKER_BYTES");
        }
        out
    }
}

/// Outcome of one check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Checked and held.
    Pass,
    /// Checked and violated, or required and not checkable.
    Fail,
    /// Nothing to check.
    Skip,
}

/// How skips are treated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Skips are reported and do not fail.
    Lenient,
    /// A skip on a core check fails (bare `--assert-slo`, CI).
    Strict,
}

/// One assertion.
#[derive(Clone, Debug, PartialEq)]
pub struct Check {
    /// Corpus path the check applies to.
    pub corpus: String,
    /// Short name.
    pub name: &'static str,
    /// Outcome.
    pub outcome: Outcome,
    /// Numbers behind the outcome.
    pub detail: String,
}

/// All checks plus the budgets not yet enforced.
#[derive(Clone, Debug, PartialEq)]
pub struct SloReport {
    /// Checks run.
    pub checks: Vec<Check>,
    /// Budgets named but not set.
    pub pending: Vec<&'static str>,
}

impl SloReport {
    /// Whether no check failed.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|c| c.outcome != Outcome::Fail)
    }

    /// The outcome of the named check on the first corpus that has it.
    #[must_use]
    pub fn outcome(&self, name: &str) -> Option<Outcome> {
        self.checks
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.outcome)
    }

    /// Human-readable lines.
    #[must_use]
    pub fn render(&self) -> String {
        let mut s = String::new();
        for c in &self.checks {
            let mark = match c.outcome {
                Outcome::Pass => "PASS",
                Outcome::Fail => "FAIL",
                Outcome::Skip => "SKIP",
            };
            let _ = writeln!(s, "{mark} {:<26} {} ({})", c.name, c.detail, c.corpus);
        }
        for p in &self.pending {
            let _ = writeln!(s, "TODO {p}: not set until the index lands");
        }
        s
    }
}

/// Parameters of the corpus CI generates when `--assert-slo` runs with no
/// results file. Sized so that a generator retaining it would exceed the RSS
/// growth bound several times over (see `tests/streaming_guard.rs`).
#[must_use]
pub fn smoke_params() -> (GenParams, WorkloadParams) {
    (
        GenParams {
            seed: 1,
            // 16 files per day so batch_id fans out; 140 800 rows per day =
            // churn * entities, so one version per entity per day.
            files: 64,
            row_groups_per_file: 2,
            rows_per_row_group: 4_400,
            entities: 281_600,
            churn: 0.5,
            days: 4,
            payload_bytes: 64,
            late_fraction: 0.02,
        },
        WorkloadParams {
            seed: 1,
            runs: 20,
            p50: 10,
            p95: 600,
            max: 2_000,
        },
    )
}

/// Generates the smoke corpus under `dir`, runs the harness, writes
/// `dir/results.json`, and returns the results.
pub fn run_smoke(dir: &Path, options: &RunOptions) -> Result<Results> {
    let (gen, wl) = smoke_params();
    let corpus = dir.join("corpus");
    eprintln!("generating smoke corpus in {}", corpus.display());
    let stats = generate(&gen, &corpus)?;
    println!("{}", describe_generator(&gen, &stats));
    generate_workload(&corpus, &wl)?;
    write_ground_truth(&corpus)?;
    run_all(&[corpus.as_path()], &dir.join("results.json"), options)
}

/// One line with the generator's memory measurements, for logs.
#[must_use]
pub fn describe_generator(params: &GenParams, stats: &GeneratorStats) -> String {
    let rss = stats.rss.as_ref().map_or_else(
        || "rss: no probe on this platform".to_string(),
        |m| {
            format!(
                "rss: method {} baseline {} B peak {} B growth {} B (bound {} B, {} samples)",
                m.method,
                m.baseline_bytes,
                m.peak_bytes,
                m.growth_bytes,
                rss_growth_bound_bytes(params),
                m.samples
            )
        },
    );
    format!(
        "generator {rss}; buffered peak {} B (bound {} B)",
        stats.peak_buffered_bytes,
        memory_bound_bytes(params)
    )
}

fn check(corpus: &str, name: &'static str, outcome: Outcome, detail: String) -> Check {
    Check {
        corpus: corpus.to_string(),
        name,
        outcome,
        detail,
    }
}

fn pass_fail(ok: bool) -> Outcome {
    if ok {
        Outcome::Pass
    } else {
        Outcome::Fail
    }
}

/// The ADR 018 generator guard: RSS growth and buffered bytes against their
/// bounds. The SLO run and the retention probe test both call this.
#[must_use]
pub fn generator_checks(
    at: &str,
    params: &GenParams,
    stats: Option<&GeneratorStats>,
) -> Vec<Check> {
    let Some(g) = stats else {
        return vec![
            check(
                at,
                "generator_rss_growth",
                Outcome::Fail,
                "generator-stats.json missing from corpus".into(),
            ),
            check(
                at,
                "generator_buffered_bound",
                Outcome::Fail,
                "generator-stats.json missing from corpus".into(),
            ),
        ];
    };
    let rss_bound = rss_growth_bound_bytes(params);
    let rel = |ok: bool| if ok { "<=" } else { ">" };
    let rss = match &g.rss {
        Some(m) => {
            let ok = m.growth_bytes <= rss_bound;
            check(
                at,
                "generator_rss_growth",
                pass_fail(ok),
                format!(
                    "growth {} B {} bound {rss_bound} B (method {}, baseline {} B, peak {} B, \
                     {} samples)",
                    m.growth_bytes,
                    rel(ok),
                    m.method,
                    m.baseline_bytes,
                    m.peak_bytes,
                    m.samples
                ),
            )
        }
        None => check(
            at,
            "generator_rss_growth",
            Outcome::Skip,
            "no RSS probe on this platform".into(),
        ),
    };
    let bound = memory_bound_bytes(params);
    let ok = g.peak_buffered_bytes <= bound;
    let buffered = check(
        at,
        "generator_buffered_bound",
        pass_fail(ok),
        format!(
            "peak buffered {} B {} bound {bound} B",
            g.peak_buffered_bytes,
            rel(ok)
        ),
    );
    vec![rss, buffered]
}

fn plan_strategies(c: &CorpusResult) -> Vec<&str> {
    c.strategies
        .iter()
        .filter(|s| s.available && s.kind == StrategyKind::Plan)
        .map(|s| s.name.as_str())
        .collect()
}

fn engine_strategies(c: &CorpusResult) -> Vec<&str> {
    c.strategies
        .iter()
        .filter(|s| s.available && s.kind == StrategyKind::Engine)
        .map(|s| s.name.as_str())
        .collect()
}

/// Results for `names` on every run, without errors.
fn ok_results<'a>(
    c: &'a CorpusResult,
    names: &'a [&'a str],
) -> impl Iterator<Item = (&'a RunResult, &'a StrategyResult)> + 'a {
    c.runs.iter().flat_map(move |run| {
        run.results
            .iter()
            .filter(move |r| r.error.is_none() && names.contains(&r.strategy.as_str()))
            .map(move |r| (run, r))
    })
}

fn summarize_violations(count: usize, compared: usize, first: Option<String>) -> String {
    format!(
        "{count} of {compared} violate{}",
        first.map(|f| format!("; first: {f}")).unwrap_or_default()
    )
}

fn completeness(c: &CorpusResult, out: &mut Vec<Check>) {
    let at = c.path.as_str();
    let available: Vec<&str> = c
        .strategies
        .iter()
        .filter(|s| s.available)
        .map(|s| s.name.as_str())
        .collect();
    let mut missing = 0usize;
    let mut errors = Vec::new();
    for run in &c.runs {
        for name in &available {
            match run.results.iter().find(|r| r.strategy == *name) {
                None => missing += 1,
                Some(r) => {
                    if let Some(e) = &r.error {
                        errors.push(format!("{} {}: {e}", run.run_id, r.strategy));
                    }
                }
            }
        }
    }
    let complete = c.runs.len() as u64 == c.workload.runs && missing == 0 && errors.is_empty();
    out.push(check(
        at,
        "harness_completes",
        pass_fail(complete),
        format!(
            "{} of {} runs, {} strategies, {missing} missing results, {} errors{}",
            c.runs.len(),
            c.workload.runs,
            available.len(),
            errors.len(),
            errors
                .first()
                .map(|e| format!("; first: {e}"))
                .unwrap_or_default()
        ),
    ));
    let plans = plan_strategies(c);
    let names = [RAW_SCAN, PARQUET_STATS];
    let present = names.iter().all(|n| plans.contains(n));
    out.push(check(
        at,
        "baselines_present",
        pass_fail(present),
        format!("plan strategies {plans:?}; raw_scan and parquet_stats required"),
    ));
}

fn plan_checks(c: &CorpusResult, out: &mut Vec<Check>) {
    let at = c.path.as_str();
    let plans = plan_strategies(c);

    let (mut compared, mut bad, mut first) = (0usize, 0usize, None);
    for (run, r) in ok_results(c, &plans) {
        compared += 1;
        if r.false_negative_row_groups != Some(0) {
            bad += 1;
            first.get_or_insert(format!(
                "{} {}: {:?}",
                run.run_id, r.strategy, r.false_negative_row_groups
            ));
        }
    }
    out.push(check(
        at,
        "no_false_negatives",
        if compared == 0 {
            Outcome::Fail
        } else {
            pass_fail(bad == 0)
        },
        summarize_violations(bad, compared, first),
    ));

    let (mut compared, mut bad, mut first) = (0usize, 0usize, None);
    for (run, r) in ok_results(c, &plans) {
        compared += 1;
        if r.cost_misreported != Some(false) {
            bad += 1;
            first.get_or_insert(format!(
                "{} {}: reported {:?}, recomputed {:?}",
                run.run_id, r.strategy, r.reported, r.cost
            ));
        }
    }
    out.push(check(
        at,
        "plan_cost_reported",
        if compared == 0 {
            Outcome::Fail
        } else {
            pass_fail(bad == 0)
        },
        summarize_violations(bad, compared, first),
    ));

    let (mut compared, mut bad, mut first) = (0usize, 0usize, None);
    if c.scan_executed {
        for (run, r) in ok_results(c, &plans) {
            compared += 1;
            let planned = r.cost.as_ref().map(|k| k.bytes);
            let ok = match (planned, r.bytes_consumed, r.bytes_read) {
                (Some(p), Some(consumed), Some(read)) => read == p && consumed == p,
                _ => false,
            };
            if !ok {
                bad += 1;
                first.get_or_insert(format!(
                    "{} {}: planned {planned:?}, consumed {:?}, read {:?}",
                    run.run_id, r.strategy, r.bytes_consumed, r.bytes_read
                ));
            }
        }
    }
    out.push(check(
        at,
        "bytes_read_matches_plan",
        if !c.scan_executed || compared == 0 {
            Outcome::Skip
        } else {
            pass_fail(bad == 0)
        },
        if c.scan_executed {
            summarize_violations(bad, compared, first)
        } else {
            "scans were off (--no-scan): no bytes were read".into()
        },
    ));
}

fn same_rows(c: &CorpusResult, out: &mut Vec<Check>) {
    let at = c.path.as_str();
    let plans = plan_strategies(c);
    let engines = engine_strategies(c);
    let (mut compared, mut bad, mut first) = (0usize, 0usize, None::<String>);
    let mut note = |bad: &mut usize, msg: String| {
        *bad += 1;
        first.get_or_insert(msg);
    };
    for run in &c.runs {
        let reference = run
            .results
            .iter()
            .find(|r| r.strategy == RAW_SCAN && r.error.is_none())
            .and_then(|r| r.rows_digest.clone());
        for r in run.results.iter().filter(|r| r.error.is_none()) {
            let is_plan = plans.contains(&r.strategy.as_str());
            let is_engine = engines.contains(&r.strategy.as_str());
            if !(is_engine || (is_plan && c.scan_executed)) {
                continue;
            }
            compared += 1;
            if r.matched_rows != Some(run.oracle.matched_rows) {
                note(
                    &mut bad,
                    format!(
                        "{} {}: {:?} rows, oracle {}",
                        run.run_id, r.strategy, r.matched_rows, run.oracle.matched_rows
                    ),
                );
            } else if r.amount_total.as_deref() != Some(run.oracle.amount_total.as_str()) {
                note(
                    &mut bad,
                    format!(
                        "{} {}: amount {:?}, oracle {}",
                        run.run_id, r.strategy, r.amount_total, run.oracle.amount_total
                    ),
                );
            } else if is_plan && r.rows_digest != reference {
                note(
                    &mut bad,
                    format!(
                        "{} {}: rows digest {:?} differs from raw_scan {reference:?}",
                        run.run_id, r.strategy, r.rows_digest
                    ),
                );
            }
        }
    }
    out.push(check(
        at,
        "same_rows",
        if compared == 0 {
            Outcome::Skip
        } else {
            pass_fail(bad == 0)
        },
        if compared == 0 {
            "no rows were read: scans were off and no engine ran".into()
        } else {
            summarize_violations(bad, compared, first)
        },
    ));
}

fn index_checks(c: &CorpusResult, budgets: &Budgets, out: &mut Vec<Check>) {
    let at = c.path.as_str();
    let index: Vec<(&RunResult, &StrategyResult)> = ok_results(c, &[INDEX]).collect();
    if budgets.any_index() && index.is_empty() {
        out.push(check(
            at,
            "index_results_present",
            Outcome::Fail,
            "index budgets are set but results.json has no index results".into(),
        ));
    }
    let p95 = |mut v: Vec<f64>| {
        v.sort_by(f64::total_cmp);
        quantile_sorted(&v, 0.95)
    };
    if let Some(budget) = budgets.index_lookup_memory_bytes {
        let samples: Vec<Option<u64>> = index.iter().map(|(_, r)| r.peak_memory_bytes).collect();
        let measured: Vec<u64> = samples.iter().flatten().copied().collect();
        let outcome = if measured.is_empty() || measured.len() != samples.len() {
            Outcome::Fail
        } else {
            pass_fail(measured.iter().all(|&m| m <= budget))
        };
        out.push(check(
            at,
            "index_lookup_memory",
            outcome,
            format!(
                "{} of {} index results measured, max {:?} B <= {budget} B",
                measured.len(),
                samples.len(),
                measured.iter().max()
            ),
        ));
    }
    if let Some(budget) = budgets.index_plan_p95_ms {
        let v = p95(index.iter().filter_map(|(_, r)| r.plan_ms).collect());
        out.push(check(
            at,
            "index_plan_p95",
            v.map_or(Outcome::Fail, |v| pass_fail(v <= budget)),
            format!("{v:?} ms <= {budget} ms"),
        ));
    }
    if let Some(budget) = budgets.index_latency_p95_ms {
        let v = p95(index
            .iter()
            .filter_map(|(_, r)| r.scan_ms.and(r.latency_ms))
            .collect());
        let outcome = if c.scan_executed {
            v.map_or(Outcome::Fail, |v| pass_fail(v <= budget))
        } else {
            Outcome::Fail
        };
        out.push(check(
            at,
            "index_latency_p95",
            outcome,
            if c.scan_executed {
                format!("{v:?} ms <= {budget} ms")
            } else {
                "the latency budget needs scanned runs; scans were off".into()
            },
        ));
    }
    if let Some(ratio) = budgets.index_max_row_groups_over_oracle {
        let ratios: Vec<f64> = index
            .iter()
            .filter_map(|(run, r)| {
                let rgs = r.cost.as_ref()?.row_groups as f64;
                Some(rgs / (run.oracle.row_groups.max(1) as f64))
            })
            .collect();
        let worst = ratios.iter().copied().fold(None, |acc: Option<f64>, x| {
            Some(acc.map_or(x, |a| a.max(x)))
        });
        out.push(check(
            at,
            "index_row_groups_vs_oracle",
            worst.map_or(Outcome::Fail, |w| pass_fail(w <= ratio)),
            format!("worst {worst:?} <= {ratio}x over {} runs", ratios.len()),
        ));
    }
    if let Some(budget) = budgets.build_memory_per_worker_bytes {
        let measured: Vec<u64> = c
            .build
            .iter()
            .filter_map(|b| b.peak_memory_per_worker_bytes)
            .collect();
        let outcome = if c.build.is_empty() || measured.len() != c.build.len() {
            Outcome::Fail
        } else {
            pass_fail(measured.iter().all(|&m| m <= budget))
        };
        out.push(check(
            at,
            "build_memory_per_worker",
            outcome,
            format!(
                "{} of {} build results measured, max {:?} B <= {budget} B",
                measured.len(),
                c.build.len(),
                measured.iter().max()
            ),
        ));
    }
}

const CORE_CHECKS: [&str; 2] = ["same_rows", "bytes_read_matches_plan"];

fn is_core(name: &str) -> bool {
    CORE_CHECKS.contains(&name) || (cfg!(target_os = "linux") && name == "generator_rss_growth")
}

/// Runs every check over `results` with the given budgets and mode.
#[must_use]
pub fn evaluate_with(results: &Results, budgets: &Budgets, mode: Mode) -> SloReport {
    let mut checks = Vec::new();
    if results.corpora.is_empty() {
        checks.push(check(
            "-",
            "harness_completes",
            Outcome::Fail,
            "results.json has no corpora".into(),
        ));
    }
    for c in &results.corpora {
        checks.extend(generator_checks(&c.path, &c.params, c.generator.as_ref()));
        completeness(c, &mut checks);
        plan_checks(c, &mut checks);
        same_rows(c, &mut checks);
        index_checks(c, budgets, &mut checks);
    }
    if mode == Mode::Strict {
        for c in &mut checks {
            if c.outcome == Outcome::Skip && is_core(c.name) {
                c.outcome = Outcome::Fail;
                c.detail = format!("strict mode: {}", c.detail);
            }
        }
    }
    SloReport {
        checks,
        pending: budgets.pending(),
    }
}

/// [`evaluate_with`] the budgets in force, lenient.
#[must_use]
pub fn evaluate(results: &Results) -> SloReport {
    evaluate_with(results, &Budgets::CURRENT, Mode::Lenient)
}

/// Evaluates with the budgets in force and turns failures into an error.
pub fn assert_slo(results: &Results, mode: Mode) -> Result<SloReport> {
    let report = evaluate_with(results, &Budgets::CURRENT, mode);
    if report.passed() {
        Ok(report)
    } else {
        Err(BenchError::Slo(report.render()))
    }
}
