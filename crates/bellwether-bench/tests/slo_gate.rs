//! The SLO gate must not pass vacuously, and the harness must not trust what
//! a plan says about itself. Each test injects one defect a reviewer would
//! probe for and asserts the specific check that has to catch it.

#![allow(clippy::unwrap_used, clippy::unnecessary_literal_bound)]

use std::path::{Path, PathBuf};

use bellwether_bench::corpus::{Corpus, GenParams};
use bellwether_bench::generate::generate;
use bellwether_bench::harness::{
    assemble, run_corpus, DuckDbMode, IndexUnderTest, RawScan, RunOptions, ScanFault,
};
use bellwether_bench::results::{BuildResult, Results};
use bellwether_bench::slo::{evaluate_with, Budgets, Mode, Outcome, SloReport};
use bellwether_bench::workload::{generate_workload, WorkloadParams};
use bellwether_bench::Key;
use bellwether_types::ScanPlan;

const NO_BUDGETS: Budgets = Budgets {
    index_plan_p95_ms: None,
    index_latency_p95_ms: None,
    index_lookup_memory_bytes: None,
    index_max_row_groups_over_oracle: None,
    build_memory_per_worker_bytes: None,
};

/// A fresh tiny corpus with a workload, in its own directory.
fn corpus(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("bellwether-bench-slo")
        .join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).unwrap();
    }
    let params = GenParams {
        seed: 3,
        files: 8,
        row_groups_per_file: 3,
        rows_per_row_group: 250,
        entities: 6_000,
        churn: 0.5,
        days: 2,
        payload_bytes: 32,
        late_fraction: 0.05,
    };
    generate(&params, &dir).unwrap();
    generate_workload(
        &dir,
        &WorkloadParams {
            seed: 2,
            runs: 20,
            p50: 3,
            p95: 50,
            max: 200,
        },
    )
    .unwrap();
    dir
}

fn options(scan: bool, fault: ScanFault) -> RunOptions {
    RunOptions {
        scan,
        duckdb: DuckDbMode::Off,
        fault,
    }
}

/// An edit applied to raw scan's plan.
type Edit = fn(&mut ScanPlan);

/// An `index` strategy that edits raw scan's plan, or panics.
struct Probe<'a> {
    inner: RawScan<'a>,
    edit: fn(&mut ScanPlan),
    panics: bool,
}

impl IndexUnderTest for Probe<'_> {
    fn name(&self) -> &str {
        "index"
    }

    fn plan(&self, keys: &[Key]) -> ScanPlan {
        assert!(!self.panics, "probe index panics on purpose");
        let mut plan = self.inner.plan(keys);
        (self.edit)(&mut plan);
        plan
    }
}

fn run_with(
    dir: &Path,
    scan: bool,
    fault: ScanFault,
    edit: Option<fn(&mut ScanPlan)>,
    panics: bool,
) -> Results {
    let corpus = Corpus::open(dir).unwrap();
    let probe = Probe {
        inner: RawScan::new(&corpus),
        edit: edit.unwrap_or(|_| {}),
        panics,
    };
    let extra: Vec<&dyn IndexUnderTest> = if edit.is_some() || panics {
        vec![&probe]
    } else {
        Vec::new()
    };
    assemble(vec![run_corpus(dir, &options(scan, fault), &extra).unwrap()])
}

/// Every check passed except RSS growth, which measures the whole process:
/// the other tests in this binary run in parallel in the same process, so
/// that check is exercised alone in `tests/streaming_guard.rs`.
fn passed_except_rss(report: &SloReport) -> bool {
    report
        .checks
        .iter()
        .filter(|c| c.name != "generator_rss_growth")
        .all(|c| c.outcome != Outcome::Fail)
}

fn outcome(report: &SloReport, name: &str) -> Outcome {
    report
        .outcome(name)
        .unwrap_or_else(|| panic!("no {name} check:\n{}", report.render()))
}

#[test]
fn clean_baselines_pass_and_budgets_without_index_results_fail() {
    let dir = corpus("vacuous");
    let results = run_with(&dir, true, ScanFault::None, None, false);
    let clean = evaluate_with(&results, &NO_BUDGETS, Mode::Strict);
    assert!(passed_except_rss(&clean), "{}", clean.render());

    // A set index budget with no index results is a failure, not a pass.
    let budgets = Budgets {
        index_plan_p95_ms: Some(1_000.0),
        ..NO_BUDGETS
    };
    let report = evaluate_with(&results, &budgets, Mode::Lenient);
    assert_eq!(outcome(&report, "index_results_present"), Outcome::Fail);
    assert_eq!(outcome(&report, "index_plan_p95"), Outcome::Fail);
    assert!(!report.passed());

    // The build budget is judged on build results; lookup results do not count.
    let budgets = Budgets {
        build_memory_per_worker_bytes: Some(1 << 30),
        ..NO_BUDGETS
    };
    let report = evaluate_with(&results, &budgets, Mode::Lenient);
    assert_eq!(outcome(&report, "build_memory_per_worker"), Outcome::Fail);
    let mut with_build = results.clone();
    with_build.corpora[0].build.push(BuildResult {
        strategy: "index".into(),
        workers: 4,
        peak_memory_per_worker_bytes: Some(1 << 20),
        elapsed_ms: Some(1.0),
    });
    let report = evaluate_with(&with_build, &budgets, Mode::Lenient);
    assert_eq!(outcome(&report, "build_memory_per_worker"), Outcome::Pass);
}

#[test]
fn index_budgets_need_samples_and_measurements() {
    let dir = corpus("samples");
    // A faithful index: budgets with samples are judged on them.
    let results = run_with(&dir, true, ScanFault::None, Some(|_| {}), false);
    let generous = Budgets {
        index_plan_p95_ms: Some(60_000.0),
        index_latency_p95_ms: Some(60_000.0),
        index_max_row_groups_over_oracle: Some(1_000.0),
        ..NO_BUDGETS
    };
    let report = evaluate_with(&results, &generous, Mode::Lenient);
    for name in [
        "index_plan_p95",
        "index_latency_p95",
        "index_row_groups_vs_oracle",
    ] {
        assert_eq!(outcome(&report, name), Outcome::Pass, "{}", report.render());
    }
    let tight = Budgets {
        index_max_row_groups_over_oracle: Some(1.0),
        ..NO_BUDGETS
    };
    let report = evaluate_with(&results, &tight, Mode::Lenient);
    assert_eq!(
        outcome(&report, "index_row_groups_vs_oracle"),
        Outcome::Fail
    );

    // The lookup-memory budget fails when the index does not measure it.
    let memory = Budgets {
        index_lookup_memory_bytes: Some(1 << 30),
        ..NO_BUDGETS
    };
    let report = evaluate_with(&results, &memory, Mode::Lenient);
    assert_eq!(outcome(&report, "index_lookup_memory"), Outcome::Fail);

    // An index whose every run failed leaves an empty sample: fail.
    let failing = run_with(&dir, true, ScanFault::None, None, true);
    let report = evaluate_with(&failing, &generous, Mode::Lenient);
    assert_eq!(outcome(&report, "index_plan_p95"), Outcome::Fail);
    assert_eq!(outcome(&report, "index_results_present"), Outcome::Fail);
}

#[test]
fn invalid_plans_are_failed_runs_not_panics() {
    let dir = corpus("invalid");
    let cases: [(&str, Edit); 3] = [
        ("unknown file", |p| {
            p.entries[0].file.path = "data/day=1999-01-01/nope.parquet".into();
        }),
        ("row group out of range", |p| {
            p.entries[0].row_groups.insert(999);
        }),
        ("stale fingerprint", |p| p.entries[0].file.size += 1),
    ];
    for (what, edit) in cases {
        let results = run_with(&dir, true, ScanFault::None, Some(edit), false);
        let c = &results.corpora[0];
        for run in &c.runs {
            let r = run.results.iter().find(|r| r.strategy == "index").unwrap();
            assert!(r.error.is_some(), "{what}: no error recorded");
            assert!(r.cost.is_none(), "{what}: an invalid plan has no cost");
        }
        let report = evaluate_with(&results, &NO_BUDGETS, Mode::Lenient);
        assert_eq!(
            outcome(&report, "harness_completes"),
            Outcome::Fail,
            "{what}"
        );
    }

    // A panicking plan() is a failed run result too.
    let results = run_with(&dir, true, ScanFault::None, None, true);
    let r = &results.corpora[0].runs[0].results;
    let index = r.iter().find(|r| r.strategy == "index").unwrap();
    assert!(index.error.as_deref().unwrap().contains("panicked"));
}

#[test]
fn misreported_cost_is_recomputed_and_flagged() {
    let dir = corpus("misreported");
    let results = run_with(
        &dir,
        true,
        ScanFault::None,
        Some(|p| {
            p.bytes /= 2;
            p.row_groups_selected = 1;
        }),
        false,
    );
    let c = &results.corpora[0];
    for run in &c.runs {
        let index = run.results.iter().find(|r| r.strategy == "index").unwrap();
        let raw = run
            .results
            .iter()
            .find(|r| r.strategy == "raw_scan")
            .unwrap();
        assert_eq!(index.error, None, "a misreported plan still runs");
        assert_eq!(index.cost_misreported, Some(true));
        assert_eq!(
            index.cost.as_ref().unwrap().bytes,
            raw.cost.as_ref().unwrap().bytes,
            "cost is recomputed from the footers"
        );
        assert_eq!(index.matched_rows, Some(run.oracle.matched_rows));
    }
    let report = evaluate_with(&results, &NO_BUDGETS, Mode::Lenient);
    assert_eq!(outcome(&report, "plan_cost_reported"), Outcome::Fail);
    assert_eq!(outcome(&report, "same_rows"), Outcome::Pass);
}

#[test]
fn scans_off_skip_row_checks_and_fail_latency_budgets() {
    let dir = corpus("noscan");
    let results = run_with(&dir, false, ScanFault::None, Some(|_| {}), false);
    let lenient = evaluate_with(&results, &NO_BUDGETS, Mode::Lenient);
    assert_eq!(outcome(&lenient, "same_rows"), Outcome::Skip);
    assert_eq!(outcome(&lenient, "bytes_read_matches_plan"), Outcome::Skip);
    assert!(passed_except_rss(&lenient), "{}", lenient.render());

    // Strict mode (CI) never passes on skipped core checks.
    let strict = evaluate_with(&results, &NO_BUDGETS, Mode::Strict);
    assert_eq!(outcome(&strict, "same_rows"), Outcome::Fail);
    assert_eq!(outcome(&strict, "bytes_read_matches_plan"), Outcome::Fail);

    // A latency budget needs scanned runs.
    let budgets = Budgets {
        index_latency_p95_ms: Some(60_000.0),
        ..NO_BUDGETS
    };
    let report = evaluate_with(&results, &budgets, Mode::Lenient);
    assert_eq!(outcome(&report, "index_latency_p95"), Outcome::Fail);
}

#[test]
fn under_reading_executor_fails_the_bytes_check() {
    let dir = corpus("underread");
    for fault in [
        ScanFault::DropFirstPlannedRowGroup,
        ScanFault::DropPayloadColumn,
    ] {
        let results = run_with(&dir, true, fault, None, false);
        let report = evaluate_with(&results, &NO_BUDGETS, Mode::Lenient);
        assert_eq!(
            outcome(&report, "bytes_read_matches_plan"),
            Outcome::Fail,
            "{fault:?}:\n{}",
            report.render()
        );
    }
    // Dropping a row group also loses rows.
    let results = run_with(&dir, true, ScanFault::DropFirstPlannedRowGroup, None, false);
    let report = evaluate_with(&results, &NO_BUDGETS, Mode::Lenient);
    assert_eq!(outcome(&report, "same_rows"), Outcome::Fail);
}

#[test]
fn over_reading_executor_fails_the_bytes_check() {
    let dir = corpus("overread");
    let results = run_with(&dir, true, ScanFault::ReadExtraRowGroup, None, false);
    for run in &results.corpora[0].runs {
        for r in run.results.iter().filter(|r| r.cost.is_some()) {
            let planned = r.cost.as_ref().unwrap().bytes;
            assert!(
                r.bytes_read.unwrap() > planned,
                "{}: no extra bytes",
                r.strategy
            );
            assert!(r.bytes_consumed.unwrap() > planned);
        }
    }
    let report = evaluate_with(&results, &NO_BUDGETS, Mode::Lenient);
    assert_eq!(
        outcome(&report, "bytes_read_matches_plan"),
        Outcome::Fail,
        "{}",
        report.render()
    );
    // The extra row group's rows are not counted, so only the bytes check
    // catches it.
    assert_eq!(outcome(&report, "same_rows"), Outcome::Pass);
}
