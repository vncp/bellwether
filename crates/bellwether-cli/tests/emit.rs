//! `bellwether locate|latest|asof --emit` end to end (WP16 items 5 and 6):
//! a real index over a bench corpus, each engine's artifact exactly as the
//! CLI wrote it, executed in that engine, and the rows and the exact
//! `amount` total it returns compared with oracles that never touch
//! bellwether: the bench's own `harness::oracle` (rows and decimal total
//! of a key set over every row group) for `locate`, and a scan of every
//! row of the corpus for `latest`, `asof`, and `--live` (every row at a
//! key's newest timestamp, ties all kept; then the tombstone test).
//!
//! - DataFusion: the printed scan description, loaded with
//!   `datafusion::from_json` and run through `CountingStore` (feature
//!   `datafusion`), whose bytes are classified against each file's layout:
//!   data equal to the plan's, the planned row groups touched and no
//!   other, the footer counted apart, nothing else read.
//! - DuckDB: the printed SQL in the `duckdb` CLI (`$BELLWETHER_DUCKDB` or
//!   `duckdb`), and, relocated with `--relocate` to an HTTP range server,
//!   its bytes measured: the planned row groups touched, the delta beyond
//!   the plan printed.
//! - Spark: the printed manifest through `shims/spark/bellwether_plan.py`
//!   (`$BELLWETHER_SPARK_PYTHON`), with every unplanned file unreadable
//!   during the run: Spark opened the planned files only.
//!
//! A missing engine prints `SKIPPED:` and the reason; with
//! `BELLWETHER_REQUIRE_DATAFUSION`, `BELLWETHER_REQUIRE_DUCKDB`, or
//! `BELLWETHER_REQUIRE_SPARK` set (CI sets all three) it fails instead.
//!
//! `measured_bytes_on_the_bench_corpus` is the release measurement the
//! crate README's table comes from (ignored by default; see its doc).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::type_complexity,
    clippy::doc_markdown,
    clippy::match_wildcard_for_single_variants,
    clippy::similar_names,
    clippy::struct_field_names,
    clippy::unnecessary_wraps,
    missing_docs
)]

mod common;
#[allow(unused_imports)]
#[path = "../../bellwether-adapters/tests/common/range_server.rs"]
mod range_server;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, RecordBatch, UInt64Array};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Decimal128Type, Field, Schema, TimeUnit, TimestampMicrosecondType, UInt64Type,
};
use bellwether::types::{EvidenceBody, Record, ScanPlan};
use bellwether::PlanResult;
use bellwether_adapters::measure::{classify, FileSpans, Realized};
use bellwether_bench::corpus::{Corpus, GenParams};
use bellwether_bench::harness::{format_cents, oracle};
use common::path;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::{json, Value as Json};

const TOMBSTONE: &str = "void";

// --- The corpus and its oracle --------------------------------------------

/// One corpus row, decoded with arrow: where it is and what it holds.
#[derive(Clone, Debug)]
struct Row {
    file: PathBuf,
    rg: u32,
    row: u64,
    out: Out,
}

/// The columns every comparison uses: `entity_id`, `version_ts` (UTC
/// microseconds), `status`, and `amount` in cents; `None` for a column a
/// projection left out.
type Out = (u64, Option<i64>, Option<String>, Option<i128>);

/// A generated bench corpus, indexed by the CLI.
struct Fixture {
    _tmp: tempfile::TempDir,
    corpus: Corpus,
    /// The data root, canonical (the index's root).
    data: PathBuf,
    /// Every data file, canonical.
    files: Vec<PathBuf>,
    rows: Vec<Row>,
    /// The queried keys: present ones and two the corpus lacks.
    keys: Vec<u64>,
    /// `keys` as a Parquet file (the `--keys` of every lookup).
    keys_path: PathBuf,
    /// The present keys only: `latest`'s key list, which leaves out a key
    /// with no version, so the file `latest --keys-file-in-query` takes.
    present_keys_path: PathBuf,
    work: PathBuf,
}

fn write_keys(path: &Path, keys: &[u64]) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "entity_id",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(UInt64Array::from(keys.to_vec())) as ArrayRef],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// The `amount` total of `rows`, as the bench oracle spells it.
fn total(rows: &[Out]) -> String {
    format_cents(rows.iter().map(|r| r.3.unwrap()).sum())
}

/// `batches` as [`Out`] rows in returned order, whatever types the engine
/// wrote (DuckDB's `UBIGINT`, Spark's `decimal(20,0)` for an unsigned
/// 64-bit column, any decimal width, any timestamp unit): each column is
/// cast exactly by arrow.
fn normalized(batches: &[RecordBatch]) -> Vec<Out> {
    let mut out = Vec::new();
    for b in batches {
        let col = |name: &str, to: &DataType| -> Option<ArrayRef> {
            b.column_by_name(name).map(|a| cast(a, to).unwrap())
        };
        let ids = col("entity_id", &DataType::UInt64).expect("entity_id is returned");
        let ids = ids.as_primitive::<UInt64Type>();
        let ts = col(
            "version_ts",
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        );
        let status = col("status", &DataType::Utf8);
        let amount = col("amount", &DataType::Decimal128(38, 2));
        for i in 0..b.num_rows() {
            out.push((
                ids.value(i),
                ts.as_ref()
                    .map(|a| a.as_primitive::<TimestampMicrosecondType>().value(i)),
                status
                    .as_ref()
                    .map(|a| a.as_string::<i32>().value(i).to_string()),
                amount
                    .as_ref()
                    .map(|a| a.as_primitive::<Decimal128Type>().value(i)),
            ));
        }
    }
    out
}

fn read_parquet(path: &Path) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// A bench corpus (the generator the bench measures), indexed with an
/// exact key and a temporal key whose tombstone is `status = void`, plus
/// `extra` keys.
fn fixture(params: &GenParams, extra: &[&str], n_keys: usize) -> Fixture {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().canonicalize().unwrap();
    let root = dir.join("corpus");
    bellwether_bench::generate::generate(params, &root).unwrap();
    let corpus = Corpus::open(&root).unwrap();
    let data = root.join("data");
    let tombstone = format!("temporal:entity_id@version_ts,tombstone=status={TOMBSTONE}");
    let mut args = vec![
        "index",
        path(&data),
        "--key",
        "exact:entity_id",
        "--key",
        &tombstone,
    ];
    for k in extra {
        args.extend(["--key", k]);
    }
    common::run(&args, 0);
    let mut files = Vec::new();
    let mut rows = Vec::new();
    for rel in &corpus.manifest.files {
        let file = root.join(rel).canonicalize().unwrap();
        let groups = ParquetRecordBatchReaderBuilder::try_new(File::open(&file).unwrap())
            .unwrap()
            .metadata()
            .num_row_groups();
        let mut first = 0u64;
        for rg in 0..groups {
            let batches: Vec<RecordBatch> =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&file).unwrap())
                    .unwrap()
                    .with_row_groups(vec![rg])
                    .build()
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
            for out in normalized(&batches) {
                rows.push(Row {
                    file: file.clone(),
                    rg: u32::try_from(rg).unwrap(),
                    row: first,
                    out,
                });
                first += 1;
            }
        }
        files.push(file);
    }
    // Keys spread over the id space, every one present, and two absent.
    let present: BTreeSet<u64> = rows.iter().map(|r| r.out.0).collect();
    let step = (present.len() / n_keys).max(1);
    let mut keys: Vec<u64> = present.iter().step_by(step).take(n_keys).copied().collect();
    let work = dir.join("work");
    std::fs::create_dir_all(&work).unwrap();
    let present_keys_path = work.join("present-keys.parquet");
    write_keys(&present_keys_path, &keys);
    for absent in [u64::MAX - 3, u64::MAX / 2 + 1] {
        assert!(!present.contains(&absent));
        keys.push(absent);
    }
    keys.sort_unstable();
    let keys_path = work.join("keys.parquet");
    write_keys(&keys_path, &keys);
    Fixture {
        _tmp: tmp,
        corpus,
        data,
        files,
        rows,
        keys,
        keys_path,
        present_keys_path,
        work,
    }
}

/// Many small files and row groups, and few keys (see [`KEYS`]): the
/// generator spreads every entity over the whole corpus, so only a plan
/// of a handful of keys skips files (the Spark check) and row groups (the
/// byte checks).
fn small() -> GenParams {
    GenParams {
        seed: 7,
        files: 24,
        row_groups_per_file: 4,
        rows_per_row_group: 75,
        entities: 1_500,
        churn: 0.5,
        days: 3,
        payload_bytes: 16,
        late_fraction: 0.05,
    }
}

/// Present keys each [`small`] fixture queries.
const KEYS: usize = 6;

impl Fixture {
    fn queried(&self) -> HashSet<u64> {
        self.keys.iter().copied().collect()
    }

    /// Every row of a queried key (`locate`'s answer).
    fn locate(&self) -> Vec<Out> {
        let keys = self.queried();
        self.rows
            .iter()
            .filter(|r| keys.contains(&r.out.0))
            .map(|r| r.out.clone())
            .collect()
    }

    /// Per queried key, every row at its newest timestamp at or before
    /// `at` (ties all kept); with `live`, then without tombstone rows.
    fn latest(&self, at: Option<i64>, live: bool) -> Vec<Out> {
        let keys = self.queried();
        let eligible =
            |r: &&Row| keys.contains(&r.out.0) && at.is_none_or(|t| r.out.1.unwrap() <= t);
        let mut newest: BTreeMap<u64, i64> = BTreeMap::new();
        for r in self.rows.iter().filter(eligible) {
            let ts = r.out.1.unwrap();
            newest
                .entry(r.out.0)
                .and_modify(|n| *n = (*n).max(ts))
                .or_insert(ts);
        }
        self.rows
            .iter()
            .filter(eligible)
            .filter(|r| newest[&r.out.0] == r.out.1.unwrap())
            .filter(|r| !live || r.out.2.as_deref() != Some(TOMBSTONE))
            .map(|r| r.out.clone())
            .collect()
    }

    /// The layout of every data file, for classifying fetched bytes.
    fn spans(&self) -> BTreeMap<String, FileSpans> {
        self.files
            .iter()
            .map(|f| {
                let p = f.display().to_string();
                (p.clone(), FileSpans::of_local(&p).unwrap())
            })
            .collect()
    }

    /// A median `version_ts`, for `asof --at`.
    fn median_ts(&self) -> i64 {
        let mut ts: Vec<i64> = self.rows.iter().map(|r| r.out.1.unwrap()).collect();
        ts.sort_unstable();
        ts[ts.len() / 2]
    }
}

/// Sorted, for a multiset comparison.
fn sorted(mut rows: Vec<Out>) -> Vec<Out> {
    rows.sort();
    rows
}

/// `rows` with only the columns of a projection kept.
fn project(rows: &[Out], ts: bool, status: bool, amount: bool) -> Vec<Out> {
    rows.iter()
        .map(|r| {
            (
                r.0,
                r.1.filter(|_| ts),
                r.2.clone().filter(|_| status),
                r.3.filter(|_| amount),
            )
        })
        .collect()
}

fn assert_same(got: Vec<Out>, want: Vec<Out>, context: &str) {
    let (got, want) = (sorted(got), sorted(want));
    if got != want {
        let g: BTreeSet<&Out> = got.iter().collect();
        let w: BTreeSet<&Out> = want.iter().collect();
        panic!(
            "{context}: {} rows, want {}; missing {:?}; extra {:?}",
            got.len(),
            want.len(),
            w.difference(&g).take(5).collect::<Vec<_>>(),
            g.difference(&w).take(5).collect::<Vec<_>>()
        );
    }
}

// --- The CLI ----------------------------------------------------------------

/// Runs `bellwether --json <args> --emit <engine> --out <out>`: the plan
/// the lookup printed (the artifact is in `out`).
fn emit(args: &[&str], engine: &str, out: &Path) -> ScanPlan {
    let mut a = vec!["--json"];
    a.extend(args);
    a.extend(["--emit", engine, "--out", path(out)]);
    let stdout = common::run(&a, 0);
    let result: PlanResult = serde_json::from_str(stdout.trim()).unwrap();
    match result.evidence.record {
        Record::Evidence(e) => match e.body {
            EvidenceBody::Plan { plan } => plan,
            other => panic!("not a plan: {other:?}"),
        },
        other => panic!("not an evidence: {other:?}"),
    }
}

/// The stderr of a refused `bellwether` run, asserting exit code `code`.
fn refused(args: &[&str], code: i32) -> String {
    let out = common::bw().args(args).output().unwrap();
    assert_eq!(
        out.status.code(),
        Some(code),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Planned `(file, row group)` pairs.
fn planned(plan: &ScanPlan) -> BTreeSet<(String, u32)> {
    plan.entries
        .iter()
        .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
        .collect()
}

/// Prints `SKIPPED: <why>` and returns `None`, or fails when `require` is
/// set in the environment.
fn skip_or_fail<T>(require: &str, why: &str) -> Option<T> {
    assert!(
        std::env::var_os(require).is_none(),
        "{require} is set: {why}"
    );
    eprintln!("SKIPPED: {why}");
    None
}

// --- DataFusion -------------------------------------------------------------

/// What one DataFusion run returned and read.
#[cfg_attr(not(feature = "datafusion"), allow(dead_code))]
struct DfRun {
    batches: Vec<RecordBatch>,
    /// What storage served.
    served: Realized,
    /// What DataFusion asked for.
    requested: Realized,
    /// `metadata_size_hint` per file, as the description printed it.
    hints: BTreeMap<String, Option<u64>>,
    /// Objects asked for or served that are not corpus files (the keys
    /// file of `--keys-file-in-query`), with their bytes.
    others: BTreeMap<String, u64>,
}

/// Runs the scan description at `desc` exactly as printed: loaded with
/// `from_json` and executed through a `CountingStore` that coalesces a
/// vectored read's ranges closer than `gap` bytes (0: as the local file
/// system serves). `None` (after `SKIPPED:`) without the `datafusion`
/// feature; the description is still loaded.
fn datafusion(desc: &Path, spans: &BTreeMap<String, FileSpans>, gap: u64) -> Option<DfRun> {
    let text = std::fs::read_to_string(desc).unwrap();
    let cfg = bellwether_adapters::datafusion::from_json(&text).unwrap();
    let hints: BTreeMap<String, Option<u64>> = cfg
        .files
        .iter()
        .map(|f| (f.plan_path.clone(), f.metadata_size_hint))
        .collect();
    #[cfg(feature = "datafusion")]
    {
        use bellwether_adapters::datafusion::engine::object_store::local::LocalFileSystem;
        use bellwether_adapters::datafusion::engine::physical_plan::common::collect;
        use bellwether_adapters::datafusion::engine::prelude::SessionContext;
        use bellwether_adapters::datafusion::{execute, register, CountingStore};
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let ctx = SessionContext::new();
        let store = Arc::new(CountingStore::with_coalescing(
            Arc::new(LocalFileSystem::new()),
            gap,
        ));
        register(&ctx, &cfg, store.clone()).unwrap();
        let batches =
            rt.block_on(async { collect(execute(&cfg, &ctx).await.unwrap()).await.unwrap() });
        let (fetches, requests) = (store.fetches(), store.requests());
        let mut others = BTreeMap::new();
        for f in fetches
            .iter()
            .chain(&requests)
            .filter(|f| !spans.contains_key(&f.path))
        {
            *others.entry(f.path.clone()).or_default() += f.range.end - f.range.start;
        }
        Some(DfRun {
            batches,
            served: classify(&fetches, spans),
            requested: classify(&requests, spans),
            hints,
            others,
        })
    }
    #[cfg(not(feature = "datafusion"))]
    {
        let _ = (spans, gap, hints);
        skip_or_fail(
            "BELLWETHER_REQUIRE_DATAFUSION",
            "the DataFusion artifact loads but is not run: built without the `datafusion` feature \
             (cargo test -p bellwether-cli --features datafusion)",
        )
    }
}

/// The headline at the storage boundary: DataFusion asked for and was
/// served exactly the plan's column chunks (the projected columns' with
/// `--columns`), touched exactly the planned row groups, read nothing but
/// those and the footers, and no unplanned file; the only other object it
/// may read is the `--keys` file the artifact semi-joins against.
#[cfg_attr(not(feature = "datafusion"), allow(dead_code))]
fn assert_exactly_the_plan(run: &DfRun, plan: &ScanPlan, context: &str) {
    for p in run.others.keys() {
        assert!(
            p.ends_with("keys.parquet"),
            "{context}: read {p}, neither a planned file nor the keys file"
        );
    }
    let planned_bytes = plan.projected_bytes.unwrap_or(plan.bytes);
    for (what, r) in [("served", &run.served), ("requested", &run.requested)] {
        assert_eq!(r.data(), planned_bytes, "{context}: {what} data bytes");
        assert_eq!(r.touched(), planned(plan), "{context}: {what} row groups");
        assert_eq!(
            r.other(),
            0,
            "{context}: {what} page-index, bloom, or gap bytes"
        );
        let files: BTreeSet<&String> = r.files.keys().collect();
        let want: BTreeSet<&String> = plan.entries.iter().map(|e| &e.file.path).collect();
        assert_eq!(files, want, "{context}: {what} files");
        for e in &plan.entries {
            let bytes = e
                .columns
                .as_ref()
                .map_or(e.bytes, |c| c.iter().map(|c| c.bytes).sum());
            assert_eq!(
                r.files[&e.file.path].data, bytes,
                "{context}: {what} {}",
                e.file.path
            );
        }
    }
}

// --- DuckDB -------------------------------------------------------------------

/// The DuckDB CLI the bench uses (`$BELLWETHER_DUCKDB` or `duckdb`), or
/// `None` after `SKIPPED:`.
fn duckdb() -> Option<String> {
    let bin = std::env::var("BELLWETHER_DUCKDB").unwrap_or_else(|_| "duckdb".into());
    match Command::new(&bin)
        .args(["-no-agent", "--version"])
        .env_remove("AI_AGENT")
        .output()
    {
        Ok(o) if o.status.success() => Some(bin),
        Ok(o) => skip_or_fail(
            "BELLWETHER_REQUIRE_DUCKDB",
            &format!(
                "`{bin} -no-agent --version` failed: {}",
                String::from_utf8_lossy(&o.stderr)
            ),
        ),
        Err(e) => skip_or_fail(
            "BELLWETHER_REQUIRE_DUCKDB",
            &format!("no `{bin}` CLI ({e}); install DuckDB or set BELLWETHER_DUCKDB"),
        ),
    }
}

/// Runs `script` in the DuckDB CLI as the bench does (`-no-agent -init
/// /dev/null`, `NUL` on Windows, `AI_AGENT` removed); any stderr output is
/// an error.
fn duckdb_run(bin: &str, script: &str) -> Result<String, String> {
    use std::io::Write as _;
    let null_init = if cfg!(windows) { "NUL" } else { "/dev/null" };
    let mut child = Command::new(bin)
        .args(["-no-agent", "-init", null_init, "-csv", "-noheader"])
        .env_remove("AI_AGENT")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .map_err(|e| e.to_string())?;
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    if !out.status.success() || !stderr.is_empty() {
        return Err(format!(
            "duckdb exited {} with stderr {stderr:?}",
            out.status
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Runs the SQL file the CLI wrote, its rows copied to Parquet and read
/// back in order.
fn duckdb_rows(bin: &str, sql_path: &Path, setup: &str) -> Vec<RecordBatch> {
    let sql = std::fs::read_to_string(sql_path).unwrap();
    let out = sql_path.with_extension("out.parquet");
    let script = format!(
        "{setup}COPY ({}\n) TO '{}' (FORMAT parquet);\n",
        sql.trim_end(),
        out.display().to_string().replace('\'', "''")
    );
    duckdb_run(bin, &script).unwrap_or_else(|e| panic!("{e}\nSQL:\n{sql}"));
    read_parquet(&out)
}

// --- Spark --------------------------------------------------------------------

fn adapters_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../bellwether-adapters")
}

/// A Python that imports `pyspark` (`$BELLWETHER_SPARK_PYTHON` or
/// `python3`), or `None` after `SKIPPED:`.
fn spark_python() -> Option<String> {
    let bin = std::env::var("BELLWETHER_SPARK_PYTHON").unwrap_or_else(|_| "python3".into());
    match Command::new(&bin).args(["-c", "import pyspark"]).output() {
        Ok(o) if o.status.success() => Some(bin),
        Ok(o) => skip_or_fail(
            "BELLWETHER_REQUIRE_SPARK",
            &format!(
                "`{bin}` cannot import pyspark ({}); set BELLWETHER_SPARK_PYTHON",
                String::from_utf8_lossy(&o.stderr).trim()
            ),
        ),
        Err(e) => skip_or_fail("BELLWETHER_REQUIRE_SPARK", &format!("no `{bin}` ({e})")),
    }
}

/// One manifest for the Spark driver, with the files it may not open.
struct SparkJob {
    name: String,
    manifest: PathBuf,
    out: PathBuf,
    deny: Vec<String>,
}

/// Runs `jobs` in one local Spark session through the adapters' driver
/// (`tests/spark_driver.py`); returns each job's error, if any.
fn spark_run(py: &str, work: &Path, jobs: &[SparkJob]) -> Vec<Option<String>> {
    let jobs_path = work.join("spark-jobs.json");
    let results = work.join("spark-results.json");
    let list: Vec<Json> = jobs
        .iter()
        .map(|j| json!({ "manifest": j.manifest, "out": j.out, "deny": j.deny }))
        .collect();
    std::fs::write(&jobs_path, serde_json::to_vec(&list).unwrap()).unwrap();
    let out = Command::new(py)
        .arg(adapters_dir().join("tests/spark_driver.py"))
        .arg(adapters_dir().join("shims/spark"))
        .arg(&jobs_path)
        .arg(&results)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "the Spark driver failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let r: Vec<Json> = serde_json::from_slice(&std::fs::read(&results).unwrap()).unwrap();
    r.iter()
        .map(|v| v["error"].as_str().map(str::to_string))
        .collect()
}

/// The rows Spark wrote under `out`.
fn spark_rows(out: &Path) -> Vec<RecordBatch> {
    let mut parts: Vec<PathBuf> = std::fs::read_dir(out)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect();
    parts.sort();
    parts.iter().flat_map(|p| read_parquet(p)).collect()
}

// --- The lookups ----------------------------------------------------------------

/// One lookup the suite emits: its CLI arguments (after the verb's root),
/// its oracle rows, and whether the rows are the oracle's exactly (`false`
/// for `--residual keys_only`, a labeled superset).
struct Lookup {
    name: &'static str,
    args: Vec<String>,
    want: Vec<Out>,
    exact: bool,
}

fn lookups(f: &Fixture) -> Vec<Lookup> {
    let d = path(&f.data).to_string();
    let k = path(&f.keys_path).to_string();
    let at = f.median_ts().to_string();
    let base = |verb: &str| vec![verb.to_string(), d.clone(), "--keys".into(), k.clone()];
    let with = |verb: &str, extra: &[&str]| {
        let mut a = base(verb);
        a.extend(extra.iter().map(|s| (*s).to_string()));
        a
    };
    let all = f.locate();
    vec![
        Lookup {
            name: "locate",
            args: base("locate"),
            want: all.clone(),
            exact: true,
        },
        Lookup {
            name: "locate --keys-file-in-query",
            args: with("locate", &["--keys-file-in-query"]),
            want: all.clone(),
            exact: true,
        },
        Lookup {
            name: "locate --columns entity_id,amount",
            args: with("locate", &["--columns", "entity_id,amount"]),
            want: project(&all, false, false, true),
            exact: true,
        },
        Lookup {
            name: "latest",
            args: base("latest"),
            want: f.latest(None, false),
            exact: true,
        },
        Lookup {
            name: "latest --live",
            args: with("latest", &["--live"]),
            want: f.latest(None, true),
            exact: true,
        },
        Lookup {
            name: "latest --keys-file-in-query",
            args: vec![
                "latest".into(),
                d.clone(),
                "--keys".into(),
                path(&f.present_keys_path).to_string(),
                "--keys-file-in-query".into(),
            ],
            want: f.latest(None, false),
            exact: true,
        },
        Lookup {
            name: "asof --at",
            args: with("asof", &["--at", &at]),
            want: f.latest(Some(f.median_ts()), false),
            exact: true,
        },
        Lookup {
            name: "asof --at --live",
            args: with("asof", &["--at", &at, "--live"]),
            want: f.latest(Some(f.median_ts()), true),
            exact: true,
        },
        Lookup {
            name: "latest --residual keys-only",
            args: with("latest", &["--residual", "keys-only"]),
            want: f.latest(None, false),
            exact: false,
        },
    ]
}

/// Checks one engine's rows for `l`: exactly the oracle's (and, for
/// `locate`, the bench oracle's row count and decimal total), or, for a
/// keys-only residual, every row of a queried key in the planned row
/// groups: a superset of the full answer, of queried keys only.
fn check(f: &Fixture, l: &Lookup, plan: &ScanPlan, got: Vec<Out>, engine: &str) {
    let context = format!("{engine} {}", l.name);
    if l.exact {
        if l.name.starts_with("locate") {
            let (_, o) = oracle(&f.corpus, &f.queried()).unwrap();
            assert_eq!(
                got.len() as u64,
                o.matched_rows,
                "{context}: rows against the bench oracle"
            );
            assert_eq!(
                total(&got),
                o.amount_total,
                "{context}: amount total against the bench oracle"
            );
        }
        assert_eq!(total(&got), total(&l.want), "{context}: amount total");
        assert_same(got, l.want.clone(), &context);
    } else {
        let keys = f.queried();
        let in_plan: BTreeSet<(String, u32)> = planned(plan);
        let want: Vec<Out> = f
            .rows
            .iter()
            .filter(|r| {
                keys.contains(&r.out.0) && in_plan.contains(&(r.file.display().to_string(), r.rg))
            })
            .map(|r| r.out.clone())
            .collect();
        let got_set: BTreeSet<&Out> = got.iter().collect();
        assert!(
            l.want.iter().all(|r| got_set.contains(r)),
            "{context}: not a superset of locate's rows"
        );
        assert_same(got, want, &context);
    }
}

// --- Tests ------------------------------------------------------------------------

#[test]
fn every_lookup_emitted_for_datafusion_and_duckdb_returns_the_oracle_rows() {
    let f = fixture(&small(), &[], KEYS);
    let spans = f.spans();
    let duck = duckdb();
    let mut ran = 0;
    let mut pruned = 0;
    for (i, l) in lookups(&f).iter().enumerate() {
        let args: Vec<&str> = l.args.iter().map(String::as_str).collect();
        let desc = f.work.join(format!("l{i}.datafusion.json"));
        let plan = emit(&args, "datafusion", &desc);
        if plan.row_groups_selected < plan.row_groups_total {
            pruned += 1;
        }
        let in_query = l.name.contains("--keys-file-in-query");
        // Every keys file the fixture writes is named `*keys.parquet`.
        let keys = "keys.parquet";
        assert_eq!(
            std::fs::read_to_string(&desc).unwrap().contains(keys),
            in_query,
            "{}: the keys file",
            l.name
        );
        if let Some(run) = datafusion(&desc, &spans, 0) {
            check(&f, l, &plan, normalized(&run.batches), "datafusion");
            assert_exactly_the_plan(&run, &plan, &format!("datafusion {}", l.name));
            assert_eq!(
                !run.others.is_empty(),
                in_query,
                "{}: DataFusion read the keys file",
                l.name
            );
            ran += 1;
        }
        let sql = f.work.join(format!("l{i}.sql"));
        assert_eq!(
            emit(&args, "duckdb", &sql),
            plan,
            "{}: the same plan",
            l.name
        );
        assert_eq!(
            std::fs::read_to_string(&sql).unwrap().contains(keys),
            in_query,
            "{}: the keys file",
            l.name
        );
        if let Some(bin) = &duck {
            check(
                &f,
                l,
                &plan,
                normalized(&duckdb_rows(bin, &sql, "")),
                "duckdb",
            );
            ran += 1;
        }
    }
    let n = lookups(&f).len();
    eprintln!("{ran} engine runs; {pruned} of {n} plans read a strict subset of the row groups");
    assert_eq!(pruned, n, "every plan prunes, or the byte checks mean less");
    // A latest key list leaves out a key with no version; a keys file that
    // holds one is refused, not silently semi-joined.
    let err = refused(
        &[
            "latest",
            path(&f.data),
            "--keys",
            path(&f.keys_path),
            "--keys-file-in-query",
            "--emit",
            "duckdb",
        ],
        2,
    );
    assert!(
        err.contains("lists only the keys with a version")
            && err.contains("emit with the inline list"),
        "{err}"
    );
}

#[test]
fn spark_runs_every_emitted_manifest_opening_only_the_planned_files() {
    let f = fixture(&small(), &[], KEYS);
    let Some(py) = spark_python() else { return };
    let all: Vec<String> = f.files.iter().map(|p| p.display().to_string()).collect();
    let mut jobs = Vec::new();
    let mut plans = Vec::new();
    let ls = lookups(&f);
    for (i, l) in ls.iter().enumerate() {
        let args: Vec<&str> = l.args.iter().map(String::as_str).collect();
        let manifest = f.work.join(format!("l{i}.spark.json"));
        let plan = emit(&args, "spark", &manifest);
        let planned_files: BTreeSet<&str> =
            plan.entries.iter().map(|e| e.file.path.as_str()).collect();
        let deny: Vec<String> = all
            .iter()
            .filter(|p| !planned_files.contains(p.as_str()))
            .cloned()
            .collect();
        assert!(
            !deny.is_empty(),
            "{}: every file planned; the check would be vacuous",
            l.name
        );
        jobs.push(SparkJob {
            name: l.name.into(),
            manifest,
            out: f.work.join(format!("l{i}.spark.out")),
            deny,
        });
        plans.push(plan);
    }
    // The control: denying a planned file fails the run, so a run that
    // passes opened no denied file.
    let mut control = SparkJob {
        name: "control".into(),
        manifest: jobs[0].manifest.clone(),
        out: f.work.join("control.spark.out"),
        deny: jobs[0].deny.clone(),
    };
    control.deny.push(plans[0].entries[0].file.path.clone());
    jobs.push(control);
    let errors = spark_run(&py, &f.work, &jobs);
    let (control_error, errors) = errors.split_last().unwrap();
    assert!(
        control_error
            .as_deref()
            .is_some_and(|e| e.contains(plans[0].entries[0].file.path.as_str())),
        "denying a planned file must fail the run: {control_error:?}"
    );
    for (((job, error), l), plan) in jobs.iter().zip(errors).zip(&ls).zip(&plans) {
        assert!(
            error.is_none(),
            "{}: {}",
            job.name,
            error.as_deref().unwrap_or("")
        );
        check(&f, l, plan, normalized(&spark_rows(&job.out)), "spark");
    }
}

#[test]
fn ordered_emission_is_refused_without_a_read_order_and_follows_it_with_one() {
    let f = fixture(&small(), &[], KEYS);
    let d = path(&f.data);
    let k = path(&f.keys_path);
    let out = f.work.join("ordered.sql");
    // No range key: no read order, refused with the plan's reason.
    let err = refused(
        &[
            "locate",
            d,
            "--keys",
            k,
            "--emit",
            "duckdb",
            "--ordered",
            "--out",
            path(&out),
        ],
        2,
    );
    assert!(
        err.contains("no read order") && err.contains("range key"),
        "{err}"
    );
    assert!(!out.exists(), "a refused emission wrote its artifact");
    // With one, the rows come back in the plan's read order.
    common::run(&["index", d, "--key", "range:version_ts"], 0);
    let args = ["locate", d, "--keys", k, "--ordered"];
    let desc = f.work.join("ordered.datafusion.json");
    let plan = emit(&args, "datafusion", &desc);
    let read_order: Vec<(String, u32)> = match &plan.order {
        Some(order) => order
            .iter()
            .map(|&(e, rg)| (plan.entries[e as usize].file.path.clone(), rg))
            .collect(),
        None => plan
            .entries
            .iter()
            .flat_map(|e| e.row_groups.iter().map(move |rg| (e.file.path.clone(), rg)))
            .collect(),
    };
    // The order is a time order: each row group's least version_ts, read
    // from the rows, never decreases along it.
    let mins: Vec<i64> = read_order
        .iter()
        .map(|(p, rg)| {
            f.rows
                .iter()
                .filter(|r| r.file.display().to_string() == *p && r.rg == *rg)
                .map(|r| r.out.1.unwrap())
                .min()
                .unwrap()
        })
        .collect();
    assert!(
        mins.windows(2).all(|w| w[0] <= w[1]),
        "not a time order: {mins:?}"
    );
    let keys = f.queried();
    let mut want: Vec<(u64, i64)> = Vec::new();
    for (p, rg) in &read_order {
        let mut rows: Vec<&Row> = f
            .rows
            .iter()
            .filter(|r| r.file.display().to_string() == *p && r.rg == *rg)
            .collect();
        rows.sort_by_key(|r| r.row);
        want.extend(
            rows.iter()
                .filter(|r| keys.contains(&r.out.0))
                .map(|r| (r.out.0, r.out.1.unwrap())),
        );
    }
    let in_order = |rows: Vec<Out>| {
        rows.into_iter()
            .map(|r| (r.0, r.1.unwrap()))
            .collect::<Vec<_>>()
    };
    if let Some(run) = datafusion(&desc, &f.spans(), 0) {
        assert_eq!(in_order(normalized(&run.batches)), want, "datafusion");
    }
    let sql = f.work.join("ordered.sql");
    assert_eq!(emit(&args, "duckdb", &sql), plan);
    if let Some(bin) = duckdb() {
        assert_eq!(
            in_order(normalized(&duckdb_rows(&bin, &sql, ""))),
            want,
            "duckdb"
        );
    }
}

#[test]
fn footer_hint_and_bytes_at_the_storage_boundary() {
    let f = fixture(&small(), &[], KEYS);
    let d = path(&f.data);
    let k = path(&f.keys_path);
    let spans = f.spans();
    // DataFusion's options are refused for the other engines, by name.
    for engine in ["duckdb", "spark"] {
        for flag in ["--footer-hint", "--engine-pruning"] {
            let err = refused(&["locate", d, "--keys", k, "--emit", engine, flag], 2);
            assert!(
                err.contains("use --emit datafusion"),
                "{engine} {flag}: {err}"
            );
        }
    }
    // The footer hint is each planned file's exact footer: one footer read
    // per file, counted apart from the data, which stays the plan's.
    let hinted = f.work.join("hinted.json");
    let plan = emit(
        &["locate", d, "--keys", k, "--footer-hint"],
        "datafusion",
        &hinted,
    );
    let plain = f.work.join("plain.json");
    assert_eq!(
        emit(&["locate", d, "--keys", k], "datafusion", &plain),
        plan
    );
    if let (Some(h), Some(p)) = (
        datafusion(&hinted, &spans, 0),
        datafusion(&plain, &spans, 0),
    ) {
        for e in &plan.entries {
            let footer = spans[&e.file.path].footer_len;
            assert_eq!(h.hints[&e.file.path], Some(footer), "{}", e.file.path);
            assert_eq!(
                h.served.files[&e.file.path].footer, footer,
                "{}: one exact footer read",
                e.file.path
            );
            assert_eq!(p.hints[&e.file.path], None);
            assert!(
                p.served.files[&e.file.path].footer >= footer,
                "{}",
                e.file.path
            );
        }
        assert_exactly_the_plan(&h, &plan, "hinted");
        assert_exactly_the_plan(&p, &plan, "unhinted");
        check(
            &f,
            &lookups(&f)[0],
            &plan,
            normalized(&h.batches),
            "datafusion hinted",
        );
        // A remote store coalesces nearby ranges: what DataFusion asked for
        // is still the plan's, and what was served beyond it is counted
        // apart, never folded into the plan's data.
        let c = datafusion(&plain, &spans, 1 << 20).unwrap();
        assert_eq!(c.requested.data(), plan.bytes);
        assert_eq!(c.requested.touched(), planned(&plan));
        assert!(c.served.data() + c.served.other() >= c.requested.data());
        eprintln!(
            "datafusion, 1 MiB coalescing: requested data {} (= planned {}), served data {} other {} footer {}",
            c.requested.data(),
            plan.bytes,
            c.served.data(),
            c.served.other(),
            c.served.footer()
        );
    }
    // DuckDB through an HTTP range server (the plan's files relocated
    // there): the planned row groups are all touched, no unplanned file's
    // data is read, and the bytes beyond the plan are printed.
    let Some(bin) = duckdb() else { return };
    if let Err(e) = duckdb_run(&bin, "INSTALL httpfs; LOAD httpfs;") {
        skip_or_fail::<()>(
            "BELLWETHER_REQUIRE_DUCKDB",
            &format!("DuckDB's httpfs is not installable here ({e})"),
        );
        return;
    }
    let server = range_server::RangeServer::start(&f.data);
    let from = server.root.display().to_string();
    let sql = f.work.join("relocated.sql");
    let rplan = emit(
        &["locate", d, "--keys", k, "--relocate", &from, &server.base],
        "duckdb",
        &sql,
    );
    assert_eq!(
        rplan, plan,
        "relocating changes where the engine reads, not the plan"
    );
    assert!(std::fs::read_to_string(&sql)
        .unwrap()
        .contains(&server.base));
    server.reset();
    check(
        &f,
        &lookups(&f)[0],
        &plan,
        normalized(&duckdb_rows(&bin, &sql, "LOAD httpfs;\n")),
        "duckdb http",
    );
    let realized = classify(&server.fetches(), &spans);
    let touched = realized.touched();
    assert!(
        touched.is_superset(&planned(&plan)),
        "DuckDB skipped a planned row group"
    );
    let planned_files: BTreeSet<&str> = plan.entries.iter().map(|e| e.file.path.as_str()).collect();
    for (p, fb) in &realized.files {
        if !planned_files.contains(p.as_str()) {
            assert_eq!(fb.data, 0, "DuckDB read data of an unplanned file {p}");
        }
    }
    eprintln!(
        "duckdb at the range server: planned {} data {} footer {} other {} row groups planned {} touched {}",
        plan.bytes,
        realized.data(),
        realized.footer(),
        realized.other(),
        planned(&plan).len(),
        touched.len()
    );
}

#[test]
fn a_text_lookup_cannot_put_its_keys_file_in_the_query() {
    let f = fixture(&small(), &["text:status,tokenizer=words"], 4);
    let d = path(&f.data);
    let err = refused(
        &[
            "locate",
            d,
            "--has",
            "status=void",
            "--emit",
            "duckdb",
            "--keys-file-in-query",
        ],
        2,
    );
    assert!(
        err.contains("no key file") && err.contains("drop the flag"),
        "{err}"
    );
}

/// The release measurement behind the crate README's table: a bench
/// corpus (10 files × 8 row groups × 20,000 rows, 256-byte payload),
/// `locate` of 10, 100, and 1,000 keys, `latest`, and `asof`, each emitted
/// for DataFusion (footer hint on) and DuckDB (relocated to the range
/// server), executed, checked against the oracle, and measured. Prints one
/// JSON line per run. Run it in release:
///
/// `cargo test --release -p bellwether-cli --features datafusion --test emit -- --ignored --nocapture measured`
#[test]
#[ignore = "a measurement; run in release (see the doc comment)"]
fn measured_bytes_on_the_bench_corpus() {
    let params = GenParams {
        seed: 7,
        files: 10,
        row_groups_per_file: 8,
        rows_per_row_group: 20_000,
        entities: 200_000,
        churn: 0.05,
        days: 10,
        payload_bytes: 256,
        late_fraction: 0.02,
    };
    for n in [10usize, 100, 1_000] {
        let f = fixture(&params, &[], n);
        let spans = f.spans();
        let d = path(&f.data).to_string();
        let k = path(&f.keys_path).to_string();
        let at = f.median_ts().to_string();
        let mut cases: Vec<(String, Vec<String>, Vec<Out>)> = vec![(
            format!("locate {n} keys"),
            vec!["locate".into(), d.clone(), "--keys".into(), k.clone()],
            f.locate(),
        )];
        if n == 100 {
            cases.push((
                "latest 100 keys".into(),
                vec!["latest".into(), d.clone(), "--keys".into(), k.clone()],
                f.latest(None, false),
            ));
            cases.push((
                "asof 100 keys".into(),
                vec![
                    "asof".into(),
                    d.clone(),
                    "--keys".into(),
                    k.clone(),
                    "--at".into(),
                    at.clone(),
                ],
                f.latest(Some(f.median_ts()), false),
            ));
        }
        let duck = duckdb();
        let server = range_server::RangeServer::start(&f.data);
        for (i, (name, args, want)) in cases.iter().enumerate() {
            let mut a: Vec<&str> = args.iter().map(String::as_str).collect();
            let desc = f.work.join(format!("m{i}.json"));
            a.push("--footer-hint");
            let plan = emit(&a, "datafusion", &desc);
            a.pop();
            let line =
                |engine: &str, served: &Realized, requested: Option<&Realized>, rows: usize| {
                    json!({
                        "case": name, "engine": engine,
                        "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
                        "planned_bytes": plan.bytes,
                        "planned_row_groups": plan.row_groups_selected,
                        "row_groups_total": plan.row_groups_total,
                        "requested_data": requested.map(Realized::data),
                        "served_data": served.data(),
                        "served_footer": served.footer(),
                        "served_other": served.other(),
                        "row_groups_touched": served.touched().len(),
                        "rows": rows,
                    })
                };
            for gap in [0u64, 1 << 20] {
                if let Some(run) = datafusion(&desc, &spans, gap) {
                    let got = normalized(&run.batches);
                    assert_same(got.clone(), want.clone(), name);
                    if gap == 0 {
                        assert_exactly_the_plan(&run, &plan, name);
                    }
                    let engine = if gap == 0 {
                        "datafusion"
                    } else {
                        "datafusion, 1 MiB coalescing store"
                    };
                    println!(
                        "{}",
                        line(engine, &run.served, Some(&run.requested), got.len())
                    );
                }
            }
            if let Some(bin) = &duck {
                let sql = f.work.join(format!("m{i}.sql"));
                let (from, base) = (server.root.display().to_string(), server.base.clone());
                a.extend(["--relocate", &from, &base]);
                assert_eq!(emit(&a, "duckdb", &sql), plan);
                server.reset();
                let got = normalized(&duckdb_rows(bin, &sql, "INSTALL httpfs; LOAD httpfs;\n"));
                assert_same(got.clone(), want.clone(), name);
                let realized = classify(&server.fetches(), &spans);
                assert!(realized.touched().is_superset(&planned(&plan)), "{name}");
                println!(
                    "{}",
                    line("duckdb (httpfs, range server)", &realized, None, got.len())
                );
            }
        }
    }
}
