//! The workload recorder: recorded runs' key sets in the cache, named by
//! their BLAKE3, and a `workload.json` manifest naming them (ADR 005).
//!
//! # Identity
//!
//! A run's key set is stored under the BLAKE3 of its payload
//! ([`RunKeys::to_payload`]). The workload's identity is the BLAKE3 of the
//! canonical JSON of its [`WorkloadManifest`]: the format, the encoding
//! version, and every run's id, template, query kind, key columns, key
//! types, key count, dropped-null count, and key-set hash, with runs sorted
//! by id. Two recordings of the same keys therefore have the same hash
//! however they were fed in (an Arrow batch or a Parquet file, in any run
//! order), and a changed key, column, type, kind, or run id changes it. The
//! manifest bytes are stored in the cache under that hash too, so a `Replay`
//! record's `workload` hash resolves to everything needed to re-run it.
//! Every read checks the BLAKE3 of the bytes it got against the hash it
//! asked for, whatever [`KeySetSource`] served them.
//!
//! `workload.json` is the same manifest plus two derived fields for humans
//! and tools: the hash and the nearest-rank key-set sizes at p50, p95, and
//! max. Reading it back recomputes both and refuses a file whose recorded
//! values disagree.
//!
//! # Templates and query kinds
//!
//! A run's template groups it in reports. Without one, the template is the
//! run's key-column set, `columns(<key columns>)`: a grouping by columns, not
//! by query; pass the query or pipeline step a run came from to group by
//! that. Every run records a query kind; only `locate` exists today, and a
//! workload naming another kind is refused (`latest` and `asof` runs are
//! reserved for when the format can price them, contract v3 backlog #14).
//!
//! # The bench workload format
//!
//! [`record_bench_workload`] reads `bellwether-bench`'s `workload.json`
//! (`format: "bellwether-bench-workload/1"`) and its run files directly, in
//! two passes. The first verifies everything the bench recorded and writes
//! nothing: each run's hash (BLAKE3 over its sorted `u64` keys as 8
//! big-endian bytes), its key count, the workload hash (BLAKE3 over the run
//! hashes in order), each run file's location inside the corpus, and, when
//! `corpus.json` sits next to it, the corpus id. Only then does the second
//! pass record the runs, re-verifying each run's hash as it reads it again.
//! A mismatch is a [`ReplayError::BenchMismatch`]. The bench hashes are
//! type-specific (`u64` only); the replay hashes above are over canonical
//! encodings of any key type, so the two differ by design and both are
//! checked.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatchReader;
use bellwether_core::segment::ColumnType;
use bellwether_store::{Cache, StoreExt};
use bellwether_types::format::ENCODING_VERSION;
use bellwether_types::{Error, Hash, Result};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

use crate::error::{ReplayError, ReplayResult};
use crate::keys::{check_distinct_columns, RunKeys, RunKeysBuilder};
use crate::report::p50_p95_max;

/// Format tag of a replay workload manifest.
pub const WORKLOAD_FORMAT: &str = "bellwether-replay-workload/1";
const WORKLOAD_FORMAT_PREFIX: &str = "bellwether-replay-workload/";
/// Format tag of `bellwether-bench`'s `workload.json`.
pub const BENCH_WORKLOAD_FORMAT: &str = "bellwether-bench-workload/1";
const BENCH_WORKLOAD_FORMAT_PREFIX: &str = "bellwether-bench-workload/";
/// The query kind of a recorded run that looks keys up (`locate`), the only
/// kind replay prices today.
pub const QUERY_KIND_LOCATE: &str = "locate";

/// One recorded run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedRun {
    /// Identifier of the run, unique within the workload.
    pub run_id: String,
    /// The group the run is reported in. Defaults to the run's key-column
    /// set, `columns(<key columns>)`: a grouping by columns, not by query;
    /// recorders that know the query or pipeline step pass it instead.
    pub template: String,
    /// What the run asked of the index: [`QUERY_KIND_LOCATE`] today, with
    /// `latest` and `asof` reserved.
    pub query_kind: String,
    /// Key columns, in tuple order.
    pub columns: Vec<String>,
    /// The dataset type each key column was encoded at.
    pub key_types: Vec<ColumnType>,
    /// Distinct non-null key tuples.
    pub keys: u64,
    /// Input rows dropped because their tuple held a null (ADR 014).
    pub null_tuples: u64,
    /// BLAKE3 of the key-set payload stored in the cache.
    pub key_set: Hash,
}

/// The canonical workload manifest; its JSON's BLAKE3 is the workload hash.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadManifest {
    /// [`WORKLOAD_FORMAT`].
    pub format: String,
    /// Canonical encoding version the keys were encoded with.
    pub encoding_version: u32,
    /// Runs, sorted by id.
    pub runs: Vec<RecordedRun>,
}

/// `workload.json`: the manifest plus its hash and key-set sizes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadFile {
    /// [`WORKLOAD_FORMAT`].
    pub format: String,
    /// The workload hash (BLAKE3 of the canonical manifest JSON).
    pub hash: Hash,
    /// Canonical encoding version.
    pub encoding_version: u32,
    /// Nearest-rank key-set sizes at p50, p95, max.
    pub keys_p50_p95_max: [u64; 3],
    /// Runs, sorted by id.
    pub runs: Vec<RecordedRun>,
}

/// A validated workload and its hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Workload {
    manifest: WorkloadManifest,
    hash: Hash,
}

impl Workload {
    /// Validates `manifest` and computes its hash. Runs must be sorted by
    /// id with no duplicates, every run must name distinct key columns with
    /// one recorded type each, and every query kind must be one this build
    /// knows ([`Error::UnknownKind`] otherwise).
    pub fn new(manifest: WorkloadManifest) -> Result<Self> {
        check_format(
            &manifest.format,
            WORKLOAD_FORMAT_PREFIX,
            WORKLOAD_FORMAT,
            "workload",
        )?;
        if manifest.encoding_version > ENCODING_VERSION {
            return Err(Error::UnsupportedVersion {
                what: "encoding",
                found: manifest.encoding_version,
                supported: ENCODING_VERSION,
            });
        }
        if manifest.encoding_version != ENCODING_VERSION {
            return Err(Error::Other(format!(
                "workload encoding version {} is not {ENCODING_VERSION}",
                manifest.encoding_version
            )));
        }
        for (i, run) in manifest.runs.iter().enumerate() {
            if run.run_id.is_empty() || run.template.is_empty() {
                return Err(Error::Other(
                    "a recorded run needs an id and a template".into(),
                ));
            }
            if run.query_kind != QUERY_KIND_LOCATE {
                return Err(Error::UnknownKind {
                    what: "query kind",
                    found: run.query_kind.clone(),
                });
            }
            if i > 0 && manifest.runs[i - 1].run_id >= run.run_id {
                return Err(Error::Other(format!(
                    "workload runs must be sorted by id without duplicates; {:?} follows {:?}",
                    run.run_id,
                    manifest.runs[i - 1].run_id
                )));
            }
            if run.columns.is_empty() || run.columns.len() != run.key_types.len() {
                return Err(Error::Other(format!(
                    "run {:?} records {} columns and {} key types",
                    run.run_id,
                    run.columns.len(),
                    run.key_types.len()
                )));
            }
            check_distinct_columns(&run.columns)?;
        }
        let hash = Hash::of_record(&manifest)?;
        Ok(Self { manifest, hash })
    }

    /// The manifest.
    #[must_use]
    pub fn manifest(&self) -> &WorkloadManifest {
        &self.manifest
    }

    /// The workload hash.
    #[must_use]
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// The runs, sorted by id.
    #[must_use]
    pub fn runs(&self) -> &[RecordedRun] {
        &self.manifest.runs
    }

    /// The run with id `run_id`.
    #[must_use]
    pub fn run(&self, run_id: &str) -> Option<&RecordedRun> {
        self.manifest
            .runs
            .binary_search_by(|r| r.run_id.as_str().cmp(run_id))
            .ok()
            .map(|i| &self.manifest.runs[i])
    }

    /// Nearest-rank key-set sizes at p50, p95, max.
    #[must_use]
    pub fn keys_p50_p95_max(&self) -> [u64; 3] {
        let mut sizes: Vec<u64> = self.manifest.runs.iter().map(|r| r.keys).collect();
        sizes.sort_unstable();
        p50_p95_max(&sizes)
    }

    /// The `workload.json` form.
    #[must_use]
    pub fn to_file(&self) -> WorkloadFile {
        WorkloadFile {
            format: self.manifest.format.clone(),
            hash: self.hash,
            encoding_version: self.manifest.encoding_version,
            keys_p50_p95_max: self.keys_p50_p95_max(),
            runs: self.manifest.runs.clone(),
        }
    }

    /// Validates a `workload.json`: its hash and sizes must equal the ones
    /// recomputed from its runs.
    pub fn from_file(file: WorkloadFile) -> Result<Self> {
        let w = Self::new(WorkloadManifest {
            format: file.format,
            encoding_version: file.encoding_version,
            runs: file.runs,
        })?;
        if w.hash != file.hash {
            return Err(Error::Other(format!(
                "workload.json records hash {} but its runs hash to {}",
                file.hash, w.hash
            )));
        }
        if w.keys_p50_p95_max() != file.keys_p50_p95_max {
            return Err(Error::Other(
                "workload.json key-set sizes disagree with its runs".into(),
            ));
        }
        Ok(w)
    }

    /// Writes `workload.json` (pretty, with a trailing newline).
    pub fn write_json(&self, path: &Path) -> Result<()> {
        let mut bytes = serde_json::to_vec_pretty(&self.to_file())?;
        bytes.push(b'\n');
        std::fs::write(path, bytes).map_err(|e| Error::Other(format!("{}: {e}", path.display())))
    }

    /// Reads and validates `workload.json`.
    pub fn read_json(path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).map_err(|e| Error::Other(format!("{}: {e}", path.display())))?;
        Self::from_file(serde_json::from_slice(&bytes)?)
    }

    /// Loads the workload stored under `hash` (by
    /// [`WorkloadRecorder::finish`]). The stored bytes must hash to `hash`
    /// and be exactly the canonical JSON of a valid manifest.
    pub fn load(keys: &dyn KeySetSource, hash: &Hash) -> Result<Self> {
        let bytes = keys.get(hash)?;
        check_content(&bytes, hash, "workload")?;
        let manifest: WorkloadManifest = serde_json::from_slice(&bytes)?;
        if serde_json::to_vec(&manifest)? != bytes.as_ref() {
            return Err(Error::Other(format!(
                "workload {hash} is not stored in canonical form"
            )));
        }
        let w = Self::new(manifest)?;
        if w.hash != *hash {
            return Err(Error::Other(format!(
                "workload {hash} hashes to {}",
                w.hash
            )));
        }
        Ok(w)
    }

    /// Loads one run's key set and checks it against the run's record: the
    /// BLAKE3 of the bytes (checked here, whatever source served them), then
    /// columns, key types, and key count.
    pub fn run_keys(&self, keys: &dyn KeySetSource, run: &RecordedRun) -> Result<RunKeys> {
        let bytes = keys.get(&run.key_set)?;
        check_content(&bytes, &run.key_set, "key set")?;
        let rk = RunKeys::from_payload(&bytes)?;
        if rk.columns() != run.columns.as_slice()
            || rk.key_types() != run.key_types.as_slice()
            || rk.len() as u64 != run.keys
        {
            return Err(Error::Other(format!(
                "key set {} does not match run {:?}'s record",
                run.key_set, run.run_id
            )));
        }
        Ok(rk)
    }
}

fn check_content(bytes: &[u8], hash: &Hash, what: &str) -> Result<()> {
    let got = Hash::of(bytes);
    if got == *hash {
        Ok(())
    } else {
        Err(Error::Other(format!(
            "{what} {hash} was served bytes that hash to {got}"
        )))
    }
}

/// Where key sets and workload manifests are read from, by hash.
pub trait KeySetSource {
    /// The bytes stored under `hash`; [`Error::NotFound`] if absent. Readers
    /// check the bytes' BLAKE3 against `hash` themselves.
    fn get(&self, hash: &Hash) -> Result<Bytes>;
}

/// Where key sets and workload manifests are written, by hash.
pub trait KeySetSink {
    /// Stores `bytes` under `hash` (their BLAKE3). Idempotent.
    fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()>;
}

impl<S: StoreExt + ?Sized> KeySetSource for Cache<S> {
    fn get(&self, hash: &Hash) -> Result<Bytes> {
        let (bytes, _age) = self.get_cached(hash)?.ok_or(Error::NotFound(*hash))?;
        Ok(bytes)
    }
}

impl<S: StoreExt + ?Sized> KeySetSink for Cache<S> {
    fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()> {
        if Hash::of(&bytes) != *hash {
            return Err(Error::Other(format!("bytes do not hash to {hash}")));
        }
        self.put_cached(hash, bytes)
    }
}

/// An in-process key-set store, for callers without an index root and for
/// tests.
#[derive(Debug, Default, Clone)]
pub struct MemoryKeySets {
    entries: Arc<Mutex<BTreeMap<Hash, Bytes>>>,
}

impl MemoryKeySets {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Entries held.
    pub fn len(&self) -> Result<usize> {
        Ok(self.lock()?.len())
    }

    /// Whether nothing is held.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.lock()?.is_empty())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, BTreeMap<Hash, Bytes>>> {
        self.entries
            .lock()
            .map_err(|_| Error::Other("key-set store lock poisoned".into()))
    }
}

impl KeySetSource for MemoryKeySets {
    fn get(&self, hash: &Hash) -> Result<Bytes> {
        self.lock()?
            .get(hash)
            .cloned()
            .ok_or(Error::NotFound(*hash))
    }
}

impl KeySetSink for MemoryKeySets {
    fn put(&self, hash: &Hash, bytes: Bytes) -> Result<()> {
        if Hash::of(&bytes) != *hash {
            return Err(Error::Other(format!("bytes do not hash to {hash}")));
        }
        self.lock()?.insert(*hash, bytes);
        Ok(())
    }
}

/// A run's keys as the recorder accepts them: an Arrow batch, or a Parquet
/// file read one batch at a time. Either way every column is a key column.
#[derive(Clone, Copy, Debug)]
pub enum KeyInput<'a> {
    /// A batch whose columns are the key columns.
    Batch(&'a RecordBatch),
    /// A Parquet file whose columns are the key columns.
    Parquet(&'a Path),
}

/// Records runs' key sets into a [`KeySetSink`] and builds the workload.
///
/// Memory (ADR 018): one run's key set at a time, plus one input batch.
pub struct WorkloadRecorder<'a> {
    sink: &'a dyn KeySetSink,
    schema: SchemaRef,
    runs: BTreeMap<String, RecordedRun>,
}

impl std::fmt::Debug for WorkloadRecorder<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkloadRecorder")
            .field("runs", &self.runs.len())
            .finish_non_exhaustive()
    }
}

/// A run encoded but not yet stored.
struct Prepared {
    run: RecordedRun,
    payload: Vec<u8>,
}

impl<'a> WorkloadRecorder<'a> {
    /// A recorder writing to `sink`, encoding keys at the types `schema`
    /// (the dataset schema, `Source::schema`) declares.
    pub fn new(sink: &'a dyn KeySetSink, schema: SchemaRef) -> Self {
        Self {
            sink,
            schema,
            runs: BTreeMap::new(),
        }
    }

    /// Records run `run_id` with the default template, the run's key-column
    /// set.
    pub fn record(&mut self, run_id: &str, keys: KeyInput<'_>) -> Result<&RecordedRun> {
        self.record_with_template(run_id, keys, None)
    }

    /// Records run `run_id`: encodes its keys at the dataset types, drops
    /// null tuples (counting them), sorts and deduplicates, stores the
    /// payload in the cache under its BLAKE3, and returns the run's record.
    /// A repeated run id is refused before anything is written.
    pub fn record_with_template(
        &mut self,
        run_id: &str,
        keys: KeyInput<'_>,
        template: Option<&str>,
    ) -> Result<&RecordedRun> {
        self.check_new(run_id)?;
        let prepared = self.prepare(run_id, keys, template)?;
        self.sink
            .put(&prepared.run.key_set, Bytes::from(prepared.payload))?;
        Ok(self.runs.entry(run_id.to_string()).or_insert(prepared.run))
    }

    fn check_new(&self, run_id: &str) -> Result<()> {
        if run_id.is_empty() {
            return Err(Error::Other("a run id must not be empty".into()));
        }
        if self.runs.contains_key(run_id) {
            return Err(Error::Other(format!("run {run_id:?} is already recorded")));
        }
        Ok(())
    }

    /// Encodes a run without storing anything.
    fn prepare(
        &self,
        run_id: &str,
        keys: KeyInput<'_>,
        template: Option<&str>,
    ) -> Result<Prepared> {
        let (built, null_tuples) = match keys {
            KeyInput::Batch(batch) => {
                let columns = field_names(&batch.schema());
                let mut b = RunKeysBuilder::new(&columns, &self.schema)?;
                b.push_batch(batch)?;
                let nulls = b.null_tuples();
                (b.finish(), nulls)
            }
            KeyInput::Parquet(path) => read_parquet_keys(path, &self.schema)?,
        };
        let payload = built.to_payload()?;
        let key_set = Hash::of(&payload);
        let columns = built.columns().to_vec();
        Ok(Prepared {
            run: RecordedRun {
                run_id: run_id.to_string(),
                template: template.map_or_else(
                    || format!("columns({})", columns.join(",")),
                    ToString::to_string,
                ),
                query_kind: QUERY_KIND_LOCATE.to_string(),
                key_types: built.key_types().to_vec(),
                columns,
                keys: built.len() as u64,
                null_tuples,
                key_set,
            },
            payload,
        })
    }

    /// Runs recorded so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.runs.len()
    }

    /// Whether no run is recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// Builds the workload, stores its manifest in the sink under the
    /// workload hash, and returns it.
    pub fn finish(self) -> Result<Workload> {
        let w = Workload::new(WorkloadManifest {
            format: WORKLOAD_FORMAT.to_string(),
            encoding_version: ENCODING_VERSION,
            runs: self.runs.into_values().collect(),
        })?;
        let bytes = serde_json::to_vec(w.manifest())?;
        self.sink.put(&w.hash(), Bytes::from(bytes))?;
        Ok(w)
    }
}

fn field_names(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

fn read_parquet_keys(path: &Path, schema: &Schema) -> Result<(RunKeys, u64)> {
    let file = File::open(path).map_err(|e| Error::Source(format!("{}: {e}", path.display())))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::Source(format!("{}: {e}", path.display())))?
        .build()
        .map_err(|e| Error::Source(format!("{}: {e}", path.display())))?;
    let columns = field_names(&reader.schema());
    let mut b = RunKeysBuilder::new(&columns, schema)?;
    for batch in reader {
        let batch = batch.map_err(|e| Error::Source(format!("{}: {e}", path.display())))?;
        b.push_batch(&batch)?;
    }
    let nulls = b.null_tuples();
    Ok((b.finish(), nulls))
}

fn check_format(found: &str, prefix: &str, expected: &str, what: &'static str) -> Result<()> {
    if found == expected {
        return Ok(());
    }
    let supported = expected
        .strip_prefix(prefix)
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(1);
    if let Some(v) = found
        .strip_prefix(prefix)
        .and_then(|v| v.parse::<u32>().ok())
    {
        if v > supported {
            return Err(Error::UnsupportedVersion {
                what,
                found: v,
                supported,
            });
        }
    }
    Err(Error::Other(format!(
        "{what} format {found:?} is not {expected:?}"
    )))
}

#[derive(Deserialize)]
struct BenchWorkload {
    format: String,
    corpus_id: Hash,
    runs: Vec<BenchRun>,
    hash: Hash,
}

#[derive(Deserialize)]
struct BenchRun {
    run_id: String,
    file: String,
    keys: u64,
    hash: Hash,
}

/// What [`record_bench_workload`] verified and recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BenchWorkloadSummary {
    /// The bench corpus the keys were drawn from.
    pub corpus_id: Hash,
    /// The bench's own workload hash (BLAKE3 over its run hashes).
    pub bench_hash: Hash,
    /// Runs recorded.
    pub runs: usize,
}

fn bench_err(run_id: Option<&str>, detail: impl Into<String>) -> ReplayError {
    ReplayError::BenchMismatch {
        run_id: run_id.map(ToString::to_string),
        detail: detail.into(),
    }
}

/// Reads `bellwether-bench`'s `workload.json` and records every run into
/// `recorder` (module docs). Run files are resolved relative to the
/// directory holding `workload_json` and must stay inside it.
///
/// Nothing is written and the recorder is untouched unless every run
/// verifies: the first pass only reads and checks, holding one run's keys at
/// a time. The second pass records; if a run file changed between the
/// passes, its re-verification fails, the recorder is still untouched, and
/// the key sets already stored for earlier runs remain in the sink as
/// unreferenced, content-addressed cache entries.
pub fn record_bench_workload(
    recorder: &mut WorkloadRecorder<'_>,
    workload_json: &Path,
) -> ReplayResult<BenchWorkloadSummary> {
    let bytes = std::fs::read(workload_json)
        .map_err(|e| Error::Source(format!("{}: {e}", workload_json.display())))?;
    let bench: BenchWorkload = serde_json::from_slice(&bytes).map_err(Error::from)?;
    match check_format(
        &bench.format,
        BENCH_WORKLOAD_FORMAT_PREFIX,
        BENCH_WORKLOAD_FORMAT,
        "bench workload",
    ) {
        Ok(()) => {}
        Err(e @ Error::UnsupportedVersion { .. }) => return Err(e.into()),
        Err(e) => return Err(bench_err(None, e.to_string())),
    }
    let root = workload_json.parent().unwrap_or_else(|| Path::new("."));
    let corpus_json = root.join("corpus.json");
    if corpus_json.exists() {
        let corpus = std::fs::read(&corpus_json)
            .map_err(|e| Error::Source(format!("{}: {e}", corpus_json.display())))?;
        if Hash::of(&corpus) != bench.corpus_id {
            return Err(bench_err(
                None,
                format!(
                    "{} was generated for corpus {}, but {} hashes to {}",
                    workload_json.display(),
                    bench.corpus_id,
                    corpus_json.display(),
                    Hash::of(&corpus)
                ),
            ));
        }
    }
    let mut all = Vec::with_capacity(32 * bench.runs.len());
    for r in &bench.runs {
        all.extend_from_slice(&r.hash.0);
    }
    if Hash::of(&all) != bench.hash {
        return Err(bench_err(
            None,
            format!(
                "workload hash {} does not match its runs ({})",
                bench.hash,
                Hash::of(&all)
            ),
        ));
    }
    // Pass 1: verify every run and every id, writing nothing.
    let mut ids = BTreeSet::new();
    let mut paths = Vec::with_capacity(bench.runs.len());
    for run in &bench.runs {
        if !ids.insert(run.run_id.as_str()) {
            return Err(bench_err(Some(&run.run_id), "the run id appears twice"));
        }
        recorder.check_new(&run.run_id)?;
        let path = run_path(root, run)?;
        verified_keys(&path, run)?;
        paths.push(path);
    }
    // Pass 2: re-read, re-verify, encode, and store each run; insert into
    // the recorder only when every run is stored.
    let mut prepared = Vec::with_capacity(bench.runs.len());
    for (run, path) in bench.runs.iter().zip(&paths) {
        let (column, keys) = verified_keys(path, run)?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                column,
                DataType::UInt64,
                false,
            )])),
            vec![Arc::new(UInt64Array::from(keys)) as ArrayRef],
        )
        .map_err(|e| bench_err(Some(&run.run_id), e.to_string()))?;
        let p = recorder.prepare(&run.run_id, KeyInput::Batch(&batch), None)?;
        recorder.sink.put(&p.run.key_set, Bytes::from(p.payload))?;
        prepared.push(p.run);
    }
    for run in prepared {
        recorder.runs.insert(run.run_id.clone(), run);
    }
    Ok(BenchWorkloadSummary {
        corpus_id: bench.corpus_id,
        bench_hash: bench.hash,
        runs: bench.runs.len(),
    })
}

/// A bench run file's path, which must stay inside the corpus directory.
fn run_path(root: &Path, run: &BenchRun) -> ReplayResult<PathBuf> {
    let rel = Path::new(&run.file);
    if rel.is_absolute()
        || rel
            .components()
            .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
    {
        return Err(bench_err(
            Some(&run.run_id),
            format!("the run file {:?} is not inside the corpus", run.file),
        ));
    }
    Ok(root.join(rel))
}

/// A bench run file's keys, verified against the run's recorded hash and
/// count.
fn verified_keys(path: &Path, run: &BenchRun) -> ReplayResult<(String, Vec<u64>)> {
    let (column, keys) = read_bench_keys(path, &run.run_id)?;
    let mut h = Vec::with_capacity(keys.len() * 8);
    for k in &keys {
        h.extend_from_slice(&k.to_be_bytes());
    }
    if Hash::of(&h) != run.hash || keys.len() as u64 != run.keys {
        return Err(bench_err(
            Some(&run.run_id),
            format!(
                "{} distinct keys hashing to {}, but workload.json records {} keys hashing to {}",
                keys.len(),
                Hash::of(&h),
                run.keys,
                run.hash
            ),
        ));
    }
    Ok((column, keys))
}

/// A bench run file's keys: one non-null `UInt64` column, sorted and
/// deduplicated (how the bench hashes them).
fn read_bench_keys(path: &Path, run_id: &str) -> ReplayResult<(String, Vec<u64>)> {
    let src = |e: &dyn std::fmt::Display| Error::Source(format!("{}: {e}", path.display()));
    let file = File::open(path).map_err(|e| src(&e))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| src(&e))?
        .build()
        .map_err(|e| src(&e))?;
    let schema = reader.schema();
    if schema.fields().len() != 1 || schema.field(0).data_type() != &DataType::UInt64 {
        return Err(bench_err(
            Some(run_id),
            format!(
                "{}: a bench run file holds one UInt64 key column, found {:?}",
                path.display(),
                schema
                    .fields()
                    .iter()
                    .map(|f| (f.name().as_str(), f.data_type().to_string()))
                    .collect::<Vec<_>>()
            ),
        ));
    }
    let column = schema.field(0).name().clone();
    let mut keys = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| src(&e))?;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| bench_err(Some(run_id), "keys are not UInt64"))?;
        if col.null_count() > 0 {
            return Err(bench_err(Some(run_id), "a bench run file has null keys"));
        }
        keys.extend(col.values().iter().copied());
    }
    keys.sort_unstable();
    keys.dedup();
    Ok((column, keys))
}
