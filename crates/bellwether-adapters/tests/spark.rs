//! Spark (WP16 items 4, 5): every lookup shape's manifest, run through
//! `shims/spark/bellwether_plan.py` in a real local Spark session, returns
//! exactly the reference evaluator's rows; a shape the shim cannot run
//! exactly is a typed refusal; and Spark opens no file but the planned
//! ones: every other corpus file is unreadable at the operating system
//! during the run, and a control run that denies a planned file fails.
//! File level only: the shim pins no row group, and Spark's bytes are not
//! reported (Hadoop's local file system counters disagreed between runs
//! of identical plans, so they measure nothing here). The shim's own
//! pytest runs too.
//!
//! Needs a Python with `pyspark` (and Java): `$BELLWETHER_SPARK_PYTHON`,
//! else `python3`. Without one the tests print `SKIPPED:` and the reason;
//! with `BELLWETHER_REQUIRE_SPARK` set they fail instead.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow::array::{Int64Array, RecordBatch};
use bellwether_adapters::{spark, AdapterError, ColumnDef, EmitOptions};
use bellwether_types::ScanPlan;
use serde_json::{json, Value as Json};

const REQUIRE: &str = "BELLWETHER_REQUIRE_SPARK";

fn shim_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("shims/spark")
}

/// A Python that imports `pyspark`, or `None` after `SKIPPED:`.
fn python() -> Option<String> {
    let bin = std::env::var("BELLWETHER_SPARK_PYTHON").unwrap_or_else(|_| "python3".into());
    match Command::new(&bin)
        .args(["-c", "import pyspark; print(pyspark.__version__)"])
        .output()
    {
        Ok(o) if o.status.success() => {
            eprintln!("pyspark {}", String::from_utf8_lossy(&o.stdout).trim());
            Some(bin)
        }
        Ok(o) => common::skip_or_fail(
            REQUIRE,
            &format!(
                "`{bin}` cannot import pyspark ({}); set BELLWETHER_SPARK_PYTHON to a Python with \
                 pyspark",
                String::from_utf8_lossy(&o.stderr).trim()
            ),
        ),
        Err(e) => common::skip_or_fail(REQUIRE, &format!("no `{bin}` ({e})")),
    }
}

fn opts(c: &common::Corpus) -> EmitOptions {
    EmitOptions {
        row_offsets: Some(common::row_offsets(c)),
        partitions: Some(common::partitions(c)),
        ..EmitOptions::default()
    }
}

/// One job for the driver.
struct Job {
    name: String,
    manifest: PathBuf,
    out: PathBuf,
    deny: Vec<String>,
}

#[derive(serde::Deserialize)]
struct Outcome {
    error: Option<String>,
}

/// Runs `jobs` in one Spark session through `spark_driver.py`.
fn run(py: &str, dir: &Path, jobs: &[Job]) -> Vec<Outcome> {
    let jobs_path = dir.join("jobs.json");
    let results_path = dir.join("results.json");
    let list: Vec<Json> = jobs
        .iter()
        .map(|j| json!({ "manifest": j.manifest, "out": j.out, "deny": j.deny }))
        .collect();
    std::fs::write(&jobs_path, serde_json::to_vec(&list).unwrap()).unwrap();
    let driver = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/spark_driver.py");
    let out = Command::new(py)
        .arg(&driver)
        .arg(shim_dir())
        .arg(&jobs_path)
        .arg(&results_path)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "the Spark driver failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&std::fs::read(&results_path).unwrap()).unwrap()
}

/// The rows Spark wrote under `out` (one directory of part files).
fn written(out: &Path) -> Vec<RecordBatch> {
    let mut parts: Vec<PathBuf> = std::fs::read_dir(out)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    parts.sort();
    parts.iter().flat_map(|p| common::read_parquet(p)).collect()
}

fn manifest_job(dir: &Path, name: &str, plan: &ScanPlan, schema: &[ColumnDef], o: &EmitOptions, all_files: &[String]) -> Job {
    let n = name.replace(|c: char| !c.is_ascii_alphanumeric(), "_");
    let m = spark::manifest(plan, schema, o).unwrap_or_else(|e| panic!("{name}: {e}"));
    let manifest = dir.join(format!("{n}.json"));
    std::fs::write(&manifest, serde_json::to_vec_pretty(&m).unwrap()).unwrap();
    let planned: BTreeSet<&str> = plan.entries.iter().map(|e| e.file.path.as_str()).collect();
    Job {
        name: name.to_string(),
        manifest,
        out: dir.join(format!("{n}.out")),
        deny: all_files.iter().filter(|p| !planned.contains(p.as_str())).cloned().collect(),
    }
}

#[test]
fn every_lookup_shape_returns_the_reference_rows_reading_only_planned_files() {
    let Some(py) = python() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(&tmp.path().join("data"));
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let schema = common::dataset();
    let columns = common::all_columns();
    let files: Vec<String> = c.files.iter().map(|f| f.path.clone()).collect();
    let mut jobs = Vec::new();
    let mut want = Vec::new();
    for case in common::cases(&c) {
        if !case.spark {
            match spark::manifest(&case.plan, &schema, &opts(&c)) {
                Err(e @ AdapterError::Unsupported { engine: "spark", .. }) => {
                    let msg = e.to_string();
                    assert!(msg.contains("emit for DataFusion"), "{}: {msg}", case.name);
                }
                other => panic!("{}: not refused: {other:?}", case.name),
            }
            continue;
        }
        jobs.push(manifest_job(&work, case.name, &case.plan, &schema, &opts(&c), &files));
        want.push(common::expected(&c, &case.plan, &columns));
    }
    let results = run(&py, &work, &jobs);
    for ((job, outcome), want) in jobs.iter().zip(&results).zip(&want) {
        // A run that opened an unplanned (denied) file fails: no error
        // means Spark read the planned files only.
        assert!(outcome.error.is_none(), "{}: {}", job.name, outcome.error.as_deref().unwrap_or(""));
        common::assert_rows(&common::rendered(&written(&job.out), &columns), want, &job.name);
    }
    assert!(jobs.len() >= 20, "{}", jobs.len());
}

#[test]
fn ordered_output_and_a_nan_key_outside_the_statistics() {
    let Some(py) = python() else { return };
    let tmp = tempfile::tempdir().unwrap();
    let c = common::corpus(&tmp.path().join("data"));
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    let files: Vec<String> = c.files.iter().map(|f| f.path.clone()).collect();
    // An interleaved read order.
    let mut plan = common::locate(&c, &["id"], &common::ids(&[1, 2, 3, 4, 5]));
    let pairs: Vec<(u32, u32)> = plan
        .entries
        .iter()
        .enumerate()
        .flat_map(|(i, e)| e.row_groups.iter().map(move |rg| (u32::try_from(i).unwrap(), rg)))
        .collect();
    let mut order: Vec<(u32, u32)> = pairs.iter().rev().copied().collect();
    order.rotate_left(1);
    plan.order = Some(order.clone());
    let o = EmitOptions {
        ordered: true,
        ..opts(&c)
    };
    let ordered = manifest_job(&work, "ordered", &plan, &common::dataset(), &o, &files);
    // A NaN key whose row group's statistics (which leave NaN out) do not
    // hold the other queried key: a reader that prunes by them loses it.
    let nan = common::nan_case(&tmp.path().join("nan"));
    let nan_opts = EmitOptions {
        row_offsets: Some(nan.offsets.clone()),
        ..EmitOptions::default()
    };
    let inline = manifest_job(&work, "nan", &nan.plan, &nan.schema, &nan_opts, &[]);
    let composite = manifest_job(&work, "nan composite", &nan.composite, &nan.schema, &nan_opts, &[]);
    // The control for the unreadable-file check: denying a planned file
    // fails the run, so a run that passes opened no denied file.
    let mut control = manifest_job(&work, "control", &plan, &common::dataset(), &opts(&c), &files);
    control.deny.push(plan.entries[0].file.path.clone());
    // The key list as a keys file (a semi-join against it): 20k keys.
    let keys: Vec<i64> = (-10_000..10_000).collect();
    let kf = work.join("keys.parquet");
    common::keys_file(&kf, &keys);
    let kplan = common::locate(&c, &["id"], &common::ids(&keys));
    let kopts = EmitOptions {
        keys_file: Some(kf),
        ..opts(&c)
    };
    let keyed = manifest_job(&work, "keys file", &kplan, &common::dataset(), &kopts, &files);
    let results = run(&py, &work, &[ordered, inline, composite, control, keyed]);
    for r in [&results[0], &results[1], &results[2], &results[4]] {
        assert!(r.error.is_none(), "{:?}", r.error);
    }
    let columns = common::all_columns();
    common::assert_rows(
        &common::rendered(&written(&work.join("keys_file.out")), &columns),
        &common::expected(&c, &kplan, &columns),
        "keys file",
    );
    let denied = results[3].error.as_deref().expect("reading a denied planned file must fail");
    // Spark reports the unreadable file by name (as not existing).
    assert!(denied.contains(plan.entries[0].file.path.as_str()), "{denied}");
    // Ordered: the rows in read position, then row number.
    let got: Vec<i64> = written(&work.join("ordered.out"))
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .map(Option::unwrap)
                .collect::<Vec<_>>()
        })
        .collect();
    let keys: BTreeSet<i64> = [1, 2, 3, 4, 5].into();
    let mut want = Vec::new();
    for (e, rg) in order {
        let path = &plan.entries[e as usize].file.path;
        let fi = c.files.iter().position(|f| &f.path == path).unwrap();
        let mut rows: Vec<&common::Row> = c.rows.iter().filter(|r| r.file == fi && r.rg == rg).collect();
        rows.sort_by_key(|r| r.row);
        for r in rows {
            if let Some(v) = r.get("id") {
                let k = i64::from_be_bytes(v[1..9].try_into().unwrap()) ^ i64::MIN;
                if keys.contains(&k) {
                    want.push(k);
                }
            }
        }
    }
    assert_eq!(got, want);
    // NaN: every matching row, the NaN included, inline and as a relation.
    for out in ["nan.out", "nan_composite.out"] {
        let got = common::rendered(&written(&work.join(out)), &["id".into(), "f".into()]);
        common::assert_rows(&got, &nan.want, out);
    }
}

#[test]
fn the_shim_pytest_passes() {
    let Some(py) = python() else { return };
    let has_pytest = Command::new(&py)
        .args(["-c", "import pytest"])
        .status()
        .is_ok_and(|s| s.success());
    if !has_pytest {
        common::skip_or_fail::<()>(REQUIRE, &format!("`{py}` has pyspark but not pytest"));
        return;
    }
    let out = Command::new(&py)
        .args(["-m", "pytest", "-q", "-p", "no:cacheprovider"])
        .arg(shim_dir().join("test_bellwether_plan.py"))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "pytest failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    eprintln!("{}", String::from_utf8_lossy(&out.stdout).lines().last().unwrap_or(""));
}
