//! The performance budget check (WP22b, `bellwether bench perf-check`).
//!
//! Compares one `bellwether-perf/1` results file (`scripts/perf.sh`) with
//! the budgets in `docs/measured/budgets.toml` (`bellwether-budgets/1`).
//! Every budget names the measurement it derives from (`measured`), and the
//! file names the measured document and `perf.json` behind it.
//!
//! The rule (`docs/benchmark.md`, 2026-10-27 amendment):
//!
//! - A **regression** is a metric past its budget in two consecutive
//!   nightly runs ([`Mode::Nightly`] with the previous run's results), or
//!   past its budget widened by the pull-request tolerance (1.5x) in one
//!   labeled pull-request run ([`Mode::PullRequest`]). A latency is past
//!   its budget when above it, a throughput when below it.
//! - One nightly run past its budget is a **blip**: reported, never failed.
//! - A budgeted metric missing from the results **fails**: a section that
//!   stopped running is never skipped silently.
//! - A budget without its measurement, a budget file whose commit its
//!   document does not report, or `--runner` without a runner baseline is a
//!   **usage error** ([`CheckError`]), never a pass.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The budget file's format tag.
pub const BUDGETS_FORMAT: &str = "bellwether-budgets/1";
/// The results file's format tag.
pub const PERF_FORMAT: &str = "bellwether-perf/1";
/// The tolerance a labeled pull-request run gets over the runner budget.
pub const PULL_REQUEST_TOLERANCE: f64 = 1.5;

/// Why the check could not run: the inputs, not the measurements, are wrong.
#[derive(Debug, thiserror::Error)]
pub enum CheckError {
    /// A file could not be read.
    #[error("cannot read {path} ({source})")]
    Read {
        /// The file.
        path: PathBuf,
        /// What failed.
        source: std::io::Error,
    },
    /// A file is not what it should be.
    #[error("{path}: {message}")]
    Invalid {
        /// The file.
        path: PathBuf,
        /// What is wrong, and what to do.
        message: String,
    },
}

/// Which way a metric is good.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// A latency or a size: lower is better.
    Lower,
    /// A throughput: higher is better.
    Higher,
}

/// One budget.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Budget {
    /// The metric id in `perf.json`.
    pub id: String,
    /// Its unit.
    pub unit: String,
    /// Which way is good.
    pub direction: Direction,
    /// The measurement the budget derives from; required.
    pub measured: Option<f64>,
    /// The budget.
    pub budget: f64,
}

/// The CI runner's own baseline (`[runner]`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Runner {
    /// Why there is no baseline yet, when there is none.
    pub status: Option<String>,
    /// The runner's `perf.json` the budgets derive from.
    pub perf: Option<String>,
    /// The commit that run measured.
    pub commit: Option<String>,
    /// The budgets.
    #[serde(default)]
    pub budget: Vec<Budget>,
}

/// A budget file (`bellwether-budgets/1`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Budgets {
    /// [`BUDGETS_FORMAT`].
    pub format: String,
    /// The measured document the budgets derive from.
    pub document: String,
    /// Its `perf.json`.
    pub perf: String,
    /// The commit measured.
    pub commit: String,
    /// The measurement machine's budgets.
    #[serde(default)]
    pub budget: Vec<Budget>,
    /// The CI runner's budgets.
    #[serde(default)]
    pub runner: Runner,
}

/// One metric of a results file; only the fields the check reads.
#[derive(Clone, Debug, Deserialize)]
pub struct Metric {
    /// Id.
    pub id: String,
    /// Value (`null` when not measured).
    pub value: Option<f64>,
}

/// A results file (`bellwether-perf/1`); only the fields the check reads.
#[derive(Clone, Debug, Deserialize)]
pub struct Perf {
    /// [`PERF_FORMAT`].
    pub format: String,
    /// Where and what was measured.
    #[serde(default)]
    pub environment: BTreeMap<String, serde_json::Value>,
    /// The metrics.
    pub metrics: Vec<Metric>,
}

impl Perf {
    fn values(&self) -> BTreeMap<&str, f64> {
        self.metrics
            .iter()
            .filter_map(|m| m.value.map(|v| (m.id.as_str(), v)))
            .collect()
    }

    fn commit(&self) -> Option<&str> {
        self.environment.get("commit").and_then(|c| c.as_str())
    }
}

/// How a run is judged.
#[derive(Clone, Debug)]
pub enum Mode {
    /// A nightly run: a regression needs this run and the previous nightly
    /// run (when there is one) both past the budget.
    Nightly {
        /// The previous nightly run's results.
        previous: Option<Perf>,
    },
    /// A labeled pull-request run: past the budget widened by
    /// `tolerance` fails at once.
    PullRequest {
        /// The widening factor ([`PULL_REQUEST_TOLERANCE`]).
        tolerance: f64,
    },
}

/// One budget's outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// Within the budget.
    Pass,
    /// Past the budget in this nightly run only: reported, not failed.
    Blip,
    /// Past the budget in two consecutive nightly runs, or past the widened
    /// budget in a pull-request run.
    Regression,
    /// Budgeted but absent from the results.
    Missing,
}

/// One row of the report.
#[derive(Clone, Debug, Serialize)]
pub struct Row {
    /// Metric id.
    pub id: String,
    /// Unit.
    pub unit: String,
    /// Which way is good.
    pub direction: Direction,
    /// The budget.
    pub budget: f64,
    /// What this run is compared against: the budget, or the budget widened
    /// by the pull-request tolerance.
    pub limit: f64,
    /// This run's value.
    pub measured: Option<f64>,
    /// The previous nightly run's value.
    pub previous: Option<f64>,
    /// The outcome.
    pub verdict: Verdict,
}

/// The check's result.
#[derive(Clone, Debug, Serialize)]
pub struct Report {
    /// Which budgets: `machine` or `runner`.
    pub budgets: &'static str,
    /// `nightly` or `pull_request`.
    pub mode: &'static str,
    /// One row per budget, in file order.
    pub rows: Vec<Row>,
}

impl Report {
    /// The rows that fail the check (regressions and missing metrics).
    #[must_use]
    pub fn failures(&self) -> Vec<&Row> {
        self.rows
            .iter()
            .filter(|r| matches!(r.verdict, Verdict::Regression | Verdict::Missing))
            .collect()
    }

    /// The report as a Markdown table, failures and blips first, for a CI
    /// step summary.
    #[must_use]
    pub fn markdown(&self) -> String {
        let mut s = String::new();
        let failed = self.failures().len();
        let blips = self
            .rows
            .iter()
            .filter(|r| r.verdict == Verdict::Blip)
            .count();
        let _ = writeln!(
            s,
            "perf-check ({} budgets, {} run): {} budgets, {failed} failing, {blips} past the budget \
             once (not failed)\n",
            self.budgets,
            self.mode.replace('_', " "),
            self.rows.len()
        );
        s.push_str("| metric | budget | limit | measured | previous | verdict |\n");
        s.push_str("|---|---:|---:|---:|---:|---|\n");
        let mut rows: Vec<&Row> = self.rows.iter().collect();
        rows.sort_by_key(|r| match r.verdict {
            Verdict::Regression | Verdict::Missing => 0,
            Verdict::Blip => 1,
            Verdict::Pass => 2,
        });
        let num = |v: Option<f64>, unit: &str| v.map_or_else(|| "-".into(), |v| format!("{v:.3} {unit}"));
        for r in rows {
            let _ = writeln!(
                s,
                "| `{}` | {} | {} | {} | {} | {} |",
                r.id,
                num(Some(r.budget), &r.unit),
                num(Some(r.limit), &r.unit),
                num(r.measured, &r.unit),
                num(r.previous, &r.unit),
                match r.verdict {
                    Verdict::Pass => "pass",
                    Verdict::Blip => "past the budget once (not failed)",
                    Verdict::Regression => "**regression**",
                    Verdict::Missing => "**missing from the results**",
                }
            );
        }
        s
    }
}

fn invalid(path: &Path, message: impl Into<String>) -> CheckError {
    CheckError::Invalid {
        path: path.to_path_buf(),
        message: message.into(),
    }
}

fn read(path: &Path) -> Result<String, CheckError> {
    std::fs::read_to_string(path).map_err(|source| CheckError::Read {
        path: path.to_path_buf(),
        source,
    })
}

/// Reads a results file, refusing any other format.
pub fn read_perf(path: &Path) -> Result<Perf, CheckError> {
    let p: Perf = serde_json::from_str(&read(path)?)
        .map_err(|e| invalid(path, format!("not a bellwether-perf/1 results file ({e})")))?;
    if p.format != PERF_FORMAT {
        return Err(invalid(
            path,
            format!("format is {:?}, not {PERF_FORMAT:?}", p.format),
        ));
    }
    Ok(p)
}

/// Reads a budget file and checks it against the documents it names, which
/// are resolved under `repo` (the repository root).
///
/// Refuses (usage error): another format; a budget without the measurement
/// it derives from; a budget file whose `commit` is not the commit of the
/// `perf.json` it names, or that its measured document never mentions (the
/// budgets were re-derived without the document that justifies them); a
/// runner baseline whose `perf.json` reports another commit.
pub fn read_budgets(path: &Path, repo: &Path) -> Result<Budgets, CheckError> {
    let b: Budgets = toml::from_str(&read(path)?)
        .map_err(|e| invalid(path, format!("not a bellwether-budgets/1 file ({e})")))?;
    if b.format != BUDGETS_FORMAT {
        return Err(invalid(
            path,
            format!("format is {:?}, not {BUDGETS_FORMAT:?}", b.format),
        ));
    }
    for x in b.budget.iter().chain(&b.runner.budget) {
        if x.measured.is_none() {
            return Err(invalid(
                path,
                format!(
                    "budget {} has no measurement entry (`measured`); every budget derives from a \
                     measurement, so regenerate the file with scripts/perf-budgets.py",
                    x.id
                ),
            ));
        }
    }
    let doc_path = repo.join(&b.document);
    let doc = read(&doc_path)?;
    let perf_path = repo.join(&b.perf);
    let perf = read_perf(&perf_path)?;
    if perf.commit() != Some(b.commit.as_str()) || !doc.contains(&b.commit) {
        return Err(invalid(
            path,
            format!(
                "the budgets are of commit {}, but {} reports {} and {} {}; re-baseline only by a \
                 pull request that changes the budgets together with the measured document that \
                 justifies them",
                b.commit,
                b.perf,
                perf.commit().unwrap_or("no commit"),
                b.document,
                if doc.contains(&b.commit) {
                    "mentions it"
                } else {
                    "never mentions it"
                }
            ),
        ));
    }
    if !b.runner.budget.is_empty() {
        let (Some(rp), Some(rc)) = (&b.runner.perf, &b.runner.commit) else {
            return Err(invalid(
                path,
                "the [runner] budgets name no `perf` file or `commit` they derive from",
            ));
        };
        let runner_perf = read_perf(&repo.join(rp))?;
        if runner_perf.commit() != Some(rc.as_str()) || !doc.contains(rc.as_str()) {
            return Err(invalid(
                path,
                format!(
                    "the [runner] budgets are of commit {rc}, but {rp} reports {} (or {} never \
                     mentions it); re-baseline the runner together with the document",
                    runner_perf.commit().unwrap_or("no commit"),
                    b.document
                ),
            ));
        }
    }
    Ok(b)
}

fn past(direction: Direction, value: f64, limit: f64) -> bool {
    match direction {
        Direction::Lower => value > limit,
        Direction::Higher => value < limit,
    }
}

/// Judges `results` against the machine budgets, or with `runner` against
/// the runner's. Errors when `runner` is asked and the file has no runner
/// baseline.
pub fn check(
    budgets: &Budgets,
    results: &Perf,
    runner: bool,
    mode: &Mode,
) -> Result<Report, String> {
    let list = if runner {
        if budgets.runner.budget.is_empty() {
            return Err(format!(
                "the budget file has no runner baseline ({}); measure one with perf.yml and \
                 commit it under [runner]",
                budgets
                    .runner
                    .status
                    .as_deref()
                    .unwrap_or("no [[runner.budget]] entries")
            ));
        }
        &budgets.runner.budget
    } else {
        &budgets.budget
    };
    let now = results.values();
    let before = match mode {
        Mode::Nightly {
            previous: Some(p), ..
        } => Some(p.values()),
        _ => None,
    };
    let rows = list
        .iter()
        .map(|b| {
            let limit = match (mode, b.direction) {
                (Mode::PullRequest { tolerance }, Direction::Lower) => b.budget * tolerance,
                (Mode::PullRequest { tolerance }, Direction::Higher) => b.budget / tolerance,
                (Mode::Nightly { .. }, _) => b.budget,
            };
            let measured = now.get(b.id.as_str()).copied();
            let previous = before.as_ref().and_then(|p| p.get(b.id.as_str()).copied());
            let verdict = match (measured, mode) {
                (None, _) => Verdict::Missing,
                (Some(v), _) if !past(b.direction, v, limit) => Verdict::Pass,
                (Some(_), Mode::PullRequest { .. }) => Verdict::Regression,
                (Some(_), Mode::Nightly { .. }) => match previous {
                    Some(p) if past(b.direction, p, limit) => Verdict::Regression,
                    _ => Verdict::Blip,
                },
            };
            Row {
                id: b.id.clone(),
                unit: b.unit.clone(),
                direction: b.direction,
                budget: b.budget,
                limit,
                measured,
                previous,
                verdict,
            }
        })
        .collect();
    Ok(Report {
        budgets: if runner { "runner" } else { "machine" },
        mode: match mode {
            Mode::Nightly { .. } => "nightly",
            Mode::PullRequest { .. } => "pull_request",
        },
        rows,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn budgets(list: &[(&str, Direction, f64)]) -> Budgets {
        Budgets {
            format: BUDGETS_FORMAT.into(),
            document: "doc.md".into(),
            perf: "perf.json".into(),
            commit: "abc".into(),
            budget: Vec::new(),
            runner: Runner {
                status: None,
                perf: Some("runner.json".into()),
                commit: Some("abc".into()),
                budget: list
                    .iter()
                    .map(|(id, d, b)| Budget {
                        id: (*id).into(),
                        unit: "ms".into(),
                        direction: *d,
                        measured: Some(b / 1.25),
                        budget: *b,
                    })
                    .collect(),
            },
        }
    }

    fn perf(values: &[(&str, f64)]) -> Perf {
        Perf {
            format: PERF_FORMAT.into(),
            environment: BTreeMap::new(),
            metrics: values
                .iter()
                .map(|(id, v)| Metric {
                    id: (*id).into(),
                    value: Some(*v),
                })
                .collect(),
        }
    }

    fn verdicts(r: &Report) -> Vec<Verdict> {
        r.rows.iter().map(|r| r.verdict).collect()
    }

    const NIGHTLY: Mode = Mode::Nightly { previous: None };

    #[test]
    fn within_budget_passes() {
        let b = budgets(&[
            ("lat", Direction::Lower, 10.0),
            ("thr", Direction::Higher, 100.0),
        ]);
        let r = check(&b, &perf(&[("lat", 9.0), ("thr", 120.0)]), true, &NIGHTLY).unwrap();
        assert_eq!(verdicts(&r), [Verdict::Pass, Verdict::Pass]);
        assert!(r.failures().is_empty());
    }

    #[test]
    fn one_nightly_blip_passes() {
        let b = budgets(&[
            ("lat", Direction::Lower, 10.0),
            ("thr", Direction::Higher, 100.0),
        ]);
        let now = perf(&[("lat", 12.0), ("thr", 80.0)]);
        let first = check(&b, &now, true, &NIGHTLY).unwrap();
        assert_eq!(verdicts(&first), [Verdict::Blip, Verdict::Blip]);
        assert!(first.failures().is_empty());
        // The previous nightly run was within budget: still a blip.
        let mode = Mode::Nightly {
            previous: Some(perf(&[("lat", 9.0), ("thr", 120.0)])),
        };
        let r = check(&b, &now, true, &mode).unwrap();
        assert_eq!(verdicts(&r), [Verdict::Blip, Verdict::Blip]);
        assert!(r.failures().is_empty());
        assert!(r.markdown().contains("past the budget once"));
    }

    #[test]
    fn two_consecutive_nightly_runs_past_the_budget_fail() {
        let b = budgets(&[
            ("lat", Direction::Lower, 10.0),
            ("thr", Direction::Higher, 100.0),
        ]);
        let mode = Mode::Nightly {
            previous: Some(perf(&[("lat", 11.0), ("thr", 90.0)])),
        };
        let r = check(&b, &perf(&[("lat", 12.0), ("thr", 80.0)]), true, &mode).unwrap();
        assert_eq!(verdicts(&r), [Verdict::Regression, Verdict::Regression]);
        assert_eq!(r.failures().len(), 2);
        assert!(r.markdown().contains("**regression**"));
    }

    #[test]
    fn a_pull_request_run_fails_only_past_the_tolerance() {
        let b = budgets(&[
            ("lat", Direction::Lower, 10.0),
            ("thr", Direction::Higher, 100.0),
        ]);
        let mode = Mode::PullRequest {
            tolerance: PULL_REQUEST_TOLERANCE,
        };
        let within = check(&b, &perf(&[("lat", 14.0), ("thr", 70.0)]), true, &mode).unwrap();
        assert_eq!(verdicts(&within), [Verdict::Pass, Verdict::Pass]);
        let past = check(&b, &perf(&[("lat", 16.0), ("thr", 60.0)]), true, &mode).unwrap();
        assert_eq!(verdicts(&past), [Verdict::Regression, Verdict::Regression]);
    }

    #[test]
    fn a_missing_metric_fails_never_skips() {
        let b = budgets(&[
            ("lat", Direction::Lower, 10.0),
            ("gone", Direction::Lower, 10.0),
        ]);
        for mode in [
            NIGHTLY,
            Mode::PullRequest {
                tolerance: PULL_REQUEST_TOLERANCE,
            },
        ] {
            let r = check(&b, &perf(&[("lat", 1.0)]), true, &mode).unwrap();
            assert_eq!(verdicts(&r), [Verdict::Pass, Verdict::Missing]);
            assert_eq!(r.failures().len(), 1);
        }
        // A null value is missing too.
        let mut p = perf(&[("lat", 1.0), ("gone", 1.0)]);
        p.metrics[1].value = None;
        let r = check(&b, &p, true, &NIGHTLY).unwrap();
        assert_eq!(r.rows[1].verdict, Verdict::Missing);
    }

    #[test]
    fn runner_without_a_baseline_is_refused() {
        let mut b = budgets(&[]);
        b.runner.status = Some("not measured yet".into());
        let e = check(&b, &perf(&[]), true, &NIGHTLY).unwrap_err();
        assert!(e.contains("no runner baseline"), "{e}");
    }

    fn write_repo(dir: &Path, budgets: &str, doc: &str, commit: &str) -> PathBuf {
        std::fs::write(dir.join("doc.md"), doc).unwrap();
        std::fs::write(
            dir.join("perf.json"),
            format!(
                r#"{{"format":"bellwether-perf/1","environment":{{"commit":"{commit}"}},"metrics":[]}}"#
            ),
        )
        .unwrap();
        let p = dir.join("budgets.toml");
        std::fs::write(&p, budgets).unwrap();
        p
    }

    const HEAD: &str = "format = \"bellwether-budgets/1\"\ndocument = \"doc.md\"\nperf = \"perf.json\"\ncommit = \"abc123\"\n";

    #[test]
    fn a_budget_without_a_measurement_entry_is_a_usage_error() {
        let dir = tempdir("no-measured");
        let file = format!(
            "{HEAD}\n[[budget]]\nid = \"lat\"\nunit = \"ms\"\ndirection = \"lower\"\nbudget = 10.0\n"
        );
        let p = write_repo(&dir, &file, "measured at abc123", "abc123");
        let e = read_budgets(&p, &dir).unwrap_err().to_string();
        assert!(e.contains("budget lat has no measurement entry"), "{e}");
        let ok = format!(
            "{HEAD}\n[[budget]]\nid = \"lat\"\nunit = \"ms\"\ndirection = \"lower\"\nmeasured = 8.0\nbudget = 10.0\n"
        );
        let p = write_repo(&dir, &ok, "measured at abc123", "abc123");
        assert_eq!(read_budgets(&p, &dir).unwrap().budget.len(), 1);
    }

    #[test]
    fn budgets_newer_than_their_document_are_refused() {
        let dir = tempdir("stale");
        // The perf.json is of another commit.
        let p = write_repo(&dir, HEAD, "measured at abc123", "old999");
        let e = read_budgets(&p, &dir).unwrap_err().to_string();
        assert!(e.contains("re-baseline only by a pull request"), "{e}");
        // The document never mentions the budgets' commit.
        let p = write_repo(&dir, HEAD, "measured at old999", "abc123");
        let e = read_budgets(&p, &dir).unwrap_err().to_string();
        assert!(e.contains("never mentions it"), "{e}");
    }

    fn tempdir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "bellwether-perfcheck-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }
}
