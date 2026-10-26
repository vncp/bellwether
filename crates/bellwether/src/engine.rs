//! [`Engine`]: the data root, the index store, and what every operation
//! shares (sources, the pinned manifest, the ledger, the cache, the clock).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::datatypes::SchemaRef;
use bellwether_build::{BuildOptions, Planner, SourceKind};
use bellwether_core::sidecar::SidecarDef;
use bellwether_ledger::Ledger;
use bellwether_source::{
    fingerprint_with_policy, ChunkedTextSource, FingerprintPolicy, LocalFs, ParquetSource,
    Partitioning, Storage as _, TextFormat,
};
use bellwether_store::{
    manifest_history_path, manifest_path, now_rfc3339, parse_rfc3339, retained_sequences,
    rfc3339_millis, Cache, Manifest, ObjectStoreBackend, Snapshot, StoreExt,
};
use bellwether_types::{Error, FileFingerprint, Hash, KeySpec, Record, Result, Source};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::results::Written;

/// Where the issue tracker lives, named by every internal-error message.
pub const ISSUE_TRACKER: &str = "https://github.com/vncp/bellwether/issues";

/// Name of the default index directory inside the data root. Discovery
/// skips names that start with `.`, so the index is never read as data.
pub const DEFAULT_INDEX_DIR: &str = ".bellwether";

/// How many times a manifest pin is retried when a commit lands between
/// reading the manifest's bytes and pinning its snapshot.
const PIN_ATTEMPTS: usize = 8;

/// The kind of files under the data root.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceFormat {
    /// Parquet files; row groups are the files' own.
    #[default]
    Parquet,
    /// Delimited text; row groups are byte-range chunks.
    Csv {
        /// Field delimiter.
        delimiter: u8,
        /// Whether each file starts with a header row.
        has_header: bool,
    },
    /// JSON lines; row groups are byte-range chunks.
    JsonLines,
}

impl SourceFormat {
    /// The build crate's source kind.
    #[must_use]
    pub fn kind(self) -> SourceKind {
        match self {
            Self::Parquet => SourceKind::Parquet,
            Self::Csv {
                delimiter,
                has_header,
            } => SourceKind::Csv {
                delimiter,
                has_header,
            },
            Self::JsonLines => SourceKind::JsonLines,
        }
    }

    fn from_kind(kind: SourceKind) -> Self {
        match kind {
            SourceKind::Parquet => Self::Parquet,
            SourceKind::Csv {
                delimiter,
                has_header,
            } => Self::Csv {
                delimiter,
                has_header,
            },
            SourceKind::JsonLines => Self::JsonLines,
        }
    }

    fn text(self) -> Option<TextFormat> {
        match self {
            Self::Parquet => None,
            Self::Csv {
                delimiter,
                has_header,
            } => Some(TextFormat::Csv {
                delimiter,
                has_header,
            }),
            Self::JsonLines => Some(TextFormat::JsonLines),
        }
    }
}

/// How to open an [`Engine`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EngineOptions {
    /// The index location: a local path or an object-store URL (`s3://`,
    /// `az://`, `gs://`, `file://`, `memory://`). Default:
    /// `<root>/.bellwether/`.
    pub index: Option<String>,
    /// The kind of files under the data root. An existing index's recorded
    /// kind must agree.
    pub format: SourceFormat,
    /// Chunk size for text files seen for the first time. `None` keeps the
    /// size the index records, or 64 MiB for a new index.
    pub chunk_size: Option<u64>,
    /// The dataset schema, overriding inference from the first file (for
    /// example when files hold one decimal column at different scales).
    /// An index records its schema at its first build; afterwards the
    /// index's schema is used to read and plan.
    pub schema: Option<SchemaRef>,
    /// Which bytes a file's fingerprint hashes when an index is first built
    /// (ADR 013): `Tail64K` (the default) or `Full`, which also detects an
    /// in-place patch outside the last 64 KiB that keeps size and mtime. An
    /// existing index keeps its recorded policy; another one is refused.
    pub fingerprint_policy: Option<FingerprintPolicy>,
    /// The folder's partitioning scheme when an index is first built, and
    /// for reads of an unindexed folder: [`Partitioning::Hive`] makes every
    /// `name=value` directory below the root a column, and
    /// [`Partitioning::Template`] every placeholder of a path template such
    /// as `{day:date}.parquet`, for every source format. The index records it in
    /// `source.partitioning`; an existing index keeps its recorded scheme,
    /// and another one (a changed template included) is refused.
    pub partitioning: Option<Partitioning>,
    /// Plan against the committed manifest at this sequence instead of the
    /// current one (`--at-manifest`), for reproducible lookups over a
    /// growing folder: `locate`, `latest`, `asof`, and `why` see exactly the
    /// files, row groups, and segments that sequence recorded. A sequence
    /// garbage collection has pruned is refused with the retained range
    /// ([`Error::NotRetained`]). Operations that change the index (`index`,
    /// `fix`) refuse to run on an engine opened at a manifest.
    pub at_manifest: Option<u64>,
    /// A fixed RFC 3339 time used as every record's `created_at` and as
    /// "now" when ages are computed, so two runs over the same inputs write
    /// byte-identical records. `None` uses the system clock.
    pub clock: Option<String>,
    /// The command-line flags that shaped how this engine reads the folder
    /// and where its index is (`--index`, `--format`, `--chunk-size`,
    /// `--schema`, `--fingerprint-policy`, `--partitioning` or
    /// `--path-template`), one argument per element, unquoted, paths made
    /// absolute, as the shared flag definition (`bellwether_mcp::EngineArgs`)
    /// renders them. The facade only assembles them into the `bellwether
    /// index` command a refusal names (`ProposeRefusal::SidecarNotProposable`).
    /// `None` for an engine a library caller opened: that command is then
    /// given only when every source option above is the default.
    pub source_flags: Option<Vec<String>>,
}

/// The pinned current manifest: its planner, its snapshot, and its content
/// hash and age, read together so they describe one commit.
pub(crate) struct Pinned {
    pub snapshot: Snapshot<ObjectStoreBackend>,
    pub planner: Planner<ObjectStoreBackend>,
    /// BLAKE3 of the manifest bytes as stored (the profile's and verify's
    /// convention).
    pub hash: Hash,
    /// The manifest bytes as stored.
    pub bytes: Bytes,
    /// Time since the commit that wrote it.
    pub age: Duration,
}

impl Pinned {
    pub fn sequence(&self) -> u64 {
        self.planner.sequence()
    }

    pub fn manifest(&self) -> &Manifest {
        self.planner.manifest()
    }

    /// Declared specs complete at the pinned sequence, and those that are
    /// not (ADR 012).
    pub fn specs(&self) -> (Vec<KeySpec>, Vec<KeySpec>) {
        self.planner
            .declared_specs()
            .iter()
            .cloned()
            .partition(|s| self.planner.completeness(s).is_some())
    }

    pub fn policy(&self) -> Result<FingerprintPolicy> {
        policy_of(self.manifest())
    }
}

pub(crate) fn policy_of(m: &Manifest) -> Result<FingerprintPolicy> {
    match m.source.fingerprint_policy.as_deref() {
        None | Some("tail64k") => Ok(FingerprintPolicy::Tail64K),
        Some("full") => Ok(FingerprintPolicy::Full),
        Some(other) => Err(Error::UnknownKind {
            what: "fingerprint policy",
            found: other.to_string(),
        }),
    }
}

/// The facade over every engine crate. Each operation is a method returning
/// a typed result that serializes with serde: that value is the CLI's
/// `--json` output and what the MCP server returns (ADR 010). No method
/// panics into its caller; an internal panic becomes an error naming the
/// issue tracker.
pub struct Engine {
    root: String,
    index: String,
    /// The index location as a local directory, when it is one.
    local_index: Option<PathBuf>,
    /// Opened on first use, so an operation that writes nothing never
    /// creates a local index directory.
    store: OnceLock<Arc<ObjectStoreBackend>>,
    options: EngineOptions,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("root", &self.root)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl Engine {
    /// Opens the data under `root` and its index.
    ///
    /// `root` is a local directory (or a `file://` URL). The engine
    /// canonicalizes it the way the build does, so every crate sees one
    /// spelling. The index lives at `options.index`, by default
    /// `<root>/.bellwether/`, on any store backend.
    pub fn open(root: &str, options: EngineOptions) -> Result<Self> {
        guard(|| Self::open_inner(root, options))
    }

    fn open_inner(root: &str, options: EngineOptions) -> Result<Self> {
        let local = match root.split_once("://") {
            None => root,
            Some(("file", rest)) => rest,
            Some((scheme, _)) => {
                return Err(Error::Source(format!(
                    "the data root {root:?} is a {scheme}:// URL, but sources read local files \
                     only in this release; copy or mount the data locally and pass its path \
                     (the index itself may live on an object store, see --index)"
                )))
            }
        };
        let canonical = std::fs::canonicalize(local).map_err(|e| {
            Error::Source(format!(
                "cannot open the data root {local:?} ({e}); pass an existing directory"
            ))
        })?;
        if !canonical.is_dir() {
            return Err(Error::Source(format!(
                "the data root {} is not a directory; pass the folder that holds the files",
                canonical.display()
            )));
        }
        let root = canonical
            .to_str()
            .ok_or_else(|| {
                Error::Source(format!(
                    "the data root {} is not valid UTF-8; rename it",
                    canonical.display()
                ))
            })?
            .to_string();
        let index = options
            .index
            .clone()
            .unwrap_or_else(|| join_path(&root, DEFAULT_INDEX_DIR));
        let mut options = options;
        if let Some(c) = &options.clock {
            // One spelling for every record: the system clock's, UTC with
            // milliseconds.
            let normalized = rfc3339_millis(c).map_err(|e| {
                Error::Other(format!("the clock {c:?} is not an RFC 3339 time ({e})"))
            })?;
            options.clock = Some(normalized);
        }
        let local_index = local_path(&index)?;
        let engine = Self {
            root,
            index,
            local_index,
            store: OnceLock::new(),
            options,
        };
        if let Some(m) = engine.manifest()? {
            let recorded = SourceKind::parse(&m.source.kind)?;
            if recorded != engine.options.format.kind() {
                return Err(Error::Source(format!(
                    "the index at {} was built over {} files, not {}; open it with the same \
                     format",
                    engine.index,
                    m.source.kind,
                    engine.options.format.kind().manifest_kind()
                )));
            }
            let recorded = Partitioning::parse(m.source.partitioning.as_deref())?;
            if let Some(asked) = &engine.options.partitioning {
                if *asked != recorded {
                    return Err(Error::Source(format!(
                        "the index at {} was built with partitioning {}, not {}; open it \
                         without --partitioning or --path-template, or build a new index",
                        engine.index,
                        recorded.label(),
                        asked.label()
                    )));
                }
            }
            if !m.source.root.is_empty() && m.source.root != engine.root {
                return Err(Error::Source(format!(
                    "the index at {} indexes {}, not {}; pass that root or another --index",
                    engine.index, m.source.root, engine.root
                )));
            }
        }
        Ok(engine)
    }

    /// The canonical data root.
    #[must_use]
    pub fn root(&self) -> &str {
        &self.root
    }

    /// The index location as given or defaulted.
    #[must_use]
    pub fn index_location(&self) -> &str {
        &self.index
    }

    /// The source format.
    #[must_use]
    pub fn format(&self) -> SourceFormat {
        self.options.format
    }

    // -- shared internals ----------------------------------------------------

    /// The index store, opened (and, for a local index, its directory
    /// created) on first use. Creating it is the first write any operation
    /// makes; if the location is not writable the error names `--index`.
    pub(crate) fn store(&self) -> Result<&Arc<ObjectStoreBackend>> {
        if let Some(s) = self.store.get() {
            return Ok(s);
        }
        let unwritable = |e: &dyn std::fmt::Display| {
            Error::Store(format!(
                "cannot write the index at {} ({e}); pass a writable location with --index",
                self.index
            ))
        };
        if let Some(dir) = &self.local_index {
            std::fs::create_dir_all(dir).map_err(|e| unwritable(&e))?;
        }
        let store =
            Arc::new(ObjectStoreBackend::from_url(&self.index).map_err(|e| unwritable(&e))?);
        Ok(self.store.get_or_init(|| store))
    }

    /// The index store if this engine can write to it: it exists or can be
    /// created, and, for a local directory, a file can be created in it.
    /// `None` otherwise (for example a read-only folder and no `--index`).
    pub(crate) fn writable_store(&self) -> Option<&Arc<ObjectStoreBackend>> {
        let Ok(store) = self.store() else {
            return None;
        };
        if let Some(dir) = &self.local_index {
            // A name no earlier run can have left behind (process id and a
            // per-call random nonce), created exclusively: only this call's
            // own successful create counts as proof, and it is removed.
            use std::hash::{BuildHasher, Hasher};
            let nonce = std::collections::hash_map::RandomState::new()
                .build_hasher()
                .finish();
            let probe = dir.join(format!(".write-probe-{}-{nonce:016x}", std::process::id()));
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&probe)
                .ok()?;
            std::fs::remove_file(&probe).ok()?;
        }
        Some(store)
    }

    /// The index store if it exists, without creating it: `None` for a local
    /// index directory that is not there yet.
    pub(crate) fn existing_store(&self) -> Result<Option<&Arc<ObjectStoreBackend>>> {
        match &self.local_index {
            Some(dir) if self.store.get().is_none() && !dir.exists() => Ok(None),
            _ => self.store().map(Some),
        }
    }

    pub(crate) fn ledger(&self) -> Result<Ledger<Arc<ObjectStoreBackend>>> {
        Ok(Ledger::new(Arc::clone(self.store()?)))
    }

    pub(crate) fn cache(&self) -> Result<Cache<ObjectStoreBackend>> {
        Ok(Cache::new(Arc::clone(self.store()?), ""))
    }

    pub(crate) fn options(&self) -> &EngineOptions {
        &self.options
    }

    /// RFC 3339 UTC with millisecond precision, the time every record this
    /// engine writes carries (each record kind, certificates included): the
    /// fixed clock when one is set, normalized to the same format.
    pub(crate) fn now(&self) -> String {
        self.options.clock.clone().unwrap_or_else(now_rfc3339)
    }

    /// "Now" in nanoseconds since the epoch, for ages: the fixed clock when
    /// one is set.
    pub(crate) fn now_nanos(&self) -> Result<i64> {
        if let Some(c) = &self.options.clock {
            return parse_rfc3339(c)?
                .timestamp_nanos_opt()
                .ok_or_else(|| Error::Other(format!("the clock {c:?} is out of range")));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Other(format!("the system clock is before 1970: {e}")))?;
        Ok(i64::try_from(now.as_nanos()).unwrap_or(i64::MAX))
    }

    /// The current manifest, if the folder is indexed.
    pub(crate) fn manifest(&self) -> Result<Option<Manifest>> {
        let Some(store) = self.existing_store()? else {
            return Ok(None);
        };
        match store.get_opt(&manifest_path(""))? {
            None => Ok(None),
            Some((bytes, _)) => Ok(Some(Manifest::from_slice(&bytes)?)),
        }
    }

    /// Pins the current manifest with its content hash, or `None` if the
    /// folder is not indexed. The bytes hashed and the snapshot pinned are
    /// the same commit (sequence and nonce agree), retried a few times if a
    /// commit lands in between. With [`EngineOptions::at_manifest`], pins
    /// that committed sequence instead, or refuses with
    /// [`Error::NotRetained`].
    pub(crate) fn pin(&self) -> Result<Option<Pinned>> {
        let Some(store) = self.existing_store()? else {
            if let Some(seq) = self.options.at_manifest {
                return Err(Error::NotRetained {
                    index: self.index.clone(),
                    requested: seq,
                    retained: Vec::new(),
                });
            }
            return Ok(None);
        };
        if let Some(seq) = self.options.at_manifest {
            return self.pin_at(store, seq).map(Some);
        }
        for _ in 0..PIN_ATTEMPTS {
            let Some((bytes, _)) = store.get_opt(&manifest_path(""))? else {
                return Ok(None);
            };
            let read = Manifest::from_slice(&bytes)?;
            let snapshot = Snapshot::open(Arc::clone(store), "")?;
            let pinned = snapshot.manifest();
            if read.sequence == pinned.sequence && read.commit_nonce == pinned.commit_nonce {
                let age = commit_age(pinned, self.now_nanos()?)?;
                let planner = Planner::from_snapshot(snapshot.clone())?;
                return Ok(Some(Pinned {
                    snapshot,
                    planner,
                    hash: Hash::of(&bytes),
                    bytes,
                    age,
                }));
            }
        }
        Err(Error::VersionConflict {
            expected: "a manifest stable while it is pinned".into(),
            found: format!("{PIN_ATTEMPTS} commits in a row; retry when the writers settle"),
        })
    }

    /// Pins the committed manifest at `seq` from the store's history
    /// (`manifests/<sequence>.json`), whose bytes are those the commit wrote,
    /// so its content hash is the one an unpinned lookup at that sequence
    /// cited.
    fn pin_at(&self, store: &Arc<ObjectStoreBackend>, seq: u64) -> Result<Pinned> {
        let refuse = || -> Result<Error> {
            Ok(Error::NotRetained {
                index: self.index.clone(),
                requested: seq,
                retained: retained_sequences(&**store, "")?,
            })
        };
        let Some(snapshot) = Snapshot::open_at(Arc::clone(store), "", seq)? else {
            return Err(refuse()?);
        };
        let bytes = match store.get_opt(&manifest_history_path("", seq))? {
            Some((b, _)) => b,
            // Not backfilled yet: the current manifest is that sequence.
            None => match store.get_opt(&manifest_path(""))? {
                Some((b, _)) => b,
                None => return Err(refuse()?),
            },
        };
        let read = Manifest::from_slice(&bytes)?;
        let pinned = snapshot.manifest();
        if read.sequence != pinned.sequence || read.commit_nonce != pinned.commit_nonce {
            // The current manifest moved on between the two reads, and the
            // history object is not written yet: ask again.
            return Err(Error::VersionConflict {
                expected: format!("manifest sequence {seq}"),
                found: format!(
                    "sequence {}; retry, the commit's history copy is being written",
                    read.sequence
                ),
            });
        }
        let age = commit_age(pinned, self.now_nanos()?)?;
        let planner = Planner::from_snapshot(snapshot.clone())?;
        Ok(Pinned {
            snapshot,
            planner,
            hash: Hash::of(&bytes),
            bytes,
            age,
        })
    }

    /// The committed manifest sequences this index retains, ascending: each
    /// can be pinned with [`EngineOptions::at_manifest`]. Empty when the
    /// folder is not indexed.
    pub fn retained_manifests(&self) -> Result<Vec<u64>> {
        guard(|| match self.existing_store()? {
            None => Ok(Vec::new()),
            Some(store) => retained_sequences(&**store, ""),
        })
    }

    /// Refuses `verb` on an engine opened at a manifest: it changes the
    /// index, which only ever moves the current manifest forward.
    pub(crate) fn require_current(&self, verb: &str) -> Result<()> {
        match self.options.at_manifest {
            None => Ok(()),
            Some(seq) => Err(Error::Other(format!(
                "{verb} changes the index, so it always acts on the current manifest; run it \
                 without --at-manifest {seq}"
            ))),
        }
    }

    /// Pins the manifest, or refuses: the folder is not indexed.
    pub(crate) fn pin_required(&self, operation: &'static str) -> Result<Pinned> {
        self.pin()?.ok_or_else(|| Error::NotIndexed {
            root: self.root.clone(),
            operation,
            needs: None,
        })
    }

    /// A source over the data root, built the way the index was when one
    /// exists: its dataset schema, its fingerprint policy, and, for text, its
    /// one recorded chunk size.
    /// The source configuration the profile folds into its footer cache key
    /// (`ProfileOptions::source_config`), describing exactly the source
    /// [`Engine::source`] builds: `parquet`, or the text format and its
    /// effective chunk size.
    pub(crate) fn source_config(&self, pinned: Option<&Pinned>) -> Result<String> {
        let format = SourceFormat::from_kind(self.options.format.kind());
        let chunk = match pinned {
            Some(p) => recorded_chunk_size(p.manifest())?,
            None => None,
        };
        let partitioning = self.partitioning(pinned)?;
        let base = match format {
            SourceFormat::Parquet => "parquet".to_string(),
            SourceFormat::Csv {
                delimiter,
                has_header,
            } => format!(
                "csv:delimiter={delimiter}:header={has_header}:chunk={}",
                chunk
                    .or(self.options.chunk_size)
                    .unwrap_or(bellwether_source::DEFAULT_CHUNK_SIZE)
            ),
            SourceFormat::JsonLines => format!(
                "jsonl:chunk={}",
                chunk
                    .or(self.options.chunk_size)
                    .unwrap_or(bellwether_source::DEFAULT_CHUNK_SIZE)
            ),
        };
        Ok(match partitioning.manifest_name() {
            Some(scheme) => format!("{base}:partitioning={scheme}"),
            None => base,
        })
    }

    /// The partitioning the source is built with: the index's recorded
    /// scheme when one is pinned, else the option (none by default).
    pub(crate) fn partitioning(&self, pinned: Option<&Pinned>) -> Result<Partitioning> {
        match pinned {
            Some(p) => Partitioning::parse(p.manifest().source.partitioning.as_deref()),
            None => Ok(self.options.partitioning.clone().unwrap_or_default()),
        }
    }

    pub(crate) fn source(&self, pinned: Option<&Pinned>) -> Result<Box<dyn Source>> {
        self.source_at(pinned, &self.root)
    }

    /// [`Engine::source`] over the folder `root`, which partitioning reads
    /// the paths below.
    pub(crate) fn source_at(&self, pinned: Option<&Pinned>, root: &str) -> Result<Box<dyn Source>> {
        let partitioning = self.partitioning(pinned)?;
        let (schema, policy, chunk) = match pinned {
            Some(p) => (
                Some(SchemaRef::clone(p.planner.schema())),
                p.policy()?,
                recorded_chunk_size(p.manifest())?,
            ),
            None => (
                self.options.schema.clone(),
                self.options.fingerprint_policy.unwrap_or_default(),
                None,
            ),
        };
        let format = SourceFormat::from_kind(self.options.format.kind());
        Ok(match format.text() {
            None => {
                let mut s = ParquetSource::new()
                    .with_fingerprint_policy(policy)
                    .with_partitioning(root, partitioning);
                if let Some(schema) = schema {
                    s = s.with_schema(schema);
                }
                Box::new(s)
            }
            Some(text) => {
                let mut s = ChunkedTextSource::new(text).with_fingerprint_policy(policy);
                for dir in self.excluded() {
                    s = s.with_excluded(dir);
                }
                if let Some(size) = chunk.or(self.options.chunk_size) {
                    s = s.with_chunk_size(size);
                }
                if let Some(schema) = &schema {
                    // The files hold the dataset schema without the
                    // partition columns their paths give.
                    s = s.with_schema(bellwether_source::physical_schema(
                        schema,
                        &partitioning.column_names(root)?,
                    ));
                }
                if partitioning.is_none() {
                    Box::new(s)
                } else {
                    Box::new(bellwether_source::Partitioned::new(
                        s,
                        root,
                        partitioning,
                        schema,
                    ))
                }
            }
        })
    }

    /// Writes a non-receipt record through the ledger and returns it with
    /// its hash, exactly as stored.
    pub(crate) fn put(&self, record: Record) -> Result<Written> {
        let hash = self.ledger()?.put(&record)?;
        Ok(Written { hash, record })
    }

    /// Reads a record and its hash.
    pub(crate) fn get(&self, hash: &Hash) -> Result<Written> {
        let record = self.ledger()?.get(hash)?;
        Ok(Written {
            hash: *hash,
            record,
        })
    }
}

/// How the folder differs from what the pinned index tracks.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FolderChanges {
    /// Files the source discovers that the index does not track.
    pub untracked: Vec<FileFingerprint>,
    /// Tracked files whose fingerprint changed (ADR 013).
    pub changed: Vec<String>,
    /// Tracked files no longer found.
    pub missing: Vec<String>,
}

impl FolderChanges {
    pub fn is_empty(&self) -> bool {
        self.untracked.is_empty() && self.changed.is_empty() && self.missing.is_empty()
    }

    /// The untracked, changed, and missing paths: the one source of the
    /// lists both [`FolderStatus`] and the stale-index error carry.
    pub fn paths(&self) -> (Vec<String>, Vec<String>, Vec<String>) {
        (
            self.untracked.iter().map(|f| f.path.clone()).collect(),
            self.changed.clone(),
            self.missing.clone(),
        )
    }
}

/// What [`Engine::folder_status`] found: how the data folder differs from
/// what its index tracks. The same lists a stale-index refusal
/// ([`bellwether_types::Error::StaleIndex`]) carries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FolderStatus {
    /// The canonical data root.
    pub root: String,
    /// The current manifest sequence, or `None` for a folder never indexed
    /// (every list is then empty: there is nothing to be stale against).
    pub sequence: Option<u64>,
    /// Files under the root the index does not track, sorted.
    pub untracked: Vec<String>,
    /// Tracked files whose size or etag/mtime changed (ADR 013), sorted.
    pub changed: Vec<String>,
    /// Tracked files no longer found, sorted.
    pub missing: Vec<String>,
}

impl FolderStatus {
    /// Whether a lookup or an apply would be refused as stale.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        !(self.untracked.is_empty() && self.changed.is_empty() && self.missing.is_empty())
    }
}

impl Engine {
    /// Lists the folder and compares it with the index, the same check
    /// `locate`, `latest`, `asof`, and `fix` run first. Reads no file
    /// contents except to fingerprint untracked files, and writes nothing
    /// (it never creates the index location).
    pub fn folder_status(&self) -> Result<FolderStatus> {
        guard(|| {
            let Some(pinned) = self.pin()? else {
                return Ok(FolderStatus {
                    root: self.root.clone(),
                    ..FolderStatus::default()
                });
            };
            let (untracked, changed, missing) =
                folder_changes(self.root(), &pinned, self.format(), &self.excluded())?.paths();
            Ok(FolderStatus {
                root: self.root.clone(),
                sequence: Some(pinned.sequence()),
                untracked,
                changed,
                missing,
            })
        })
    }
}

/// A source restricted to the files a manifest recorded: `discover` lists
/// them (by path, as the manifest's fingerprints) instead of the folder, and
/// every other call goes to the folder's source, which verifies each file
/// against that fingerprint before reading it. Used when an operation is
/// pinned to an earlier manifest, so files that arrived later are not part
/// of its dataset.
pub(crate) struct SnapshotFiles {
    inner: Box<dyn Source>,
    files: Vec<FileFingerprint>,
}

impl SnapshotFiles {
    pub(crate) fn new(inner: Box<dyn Source>, manifest: &Manifest) -> Self {
        let mut files: Vec<FileFingerprint> = manifest
            .rowgroup_space
            .files
            .iter()
            .map(|f| f.fingerprint.clone())
            .collect();
        files.sort_by(|a, b| a.path.cmp(&b.path));
        Self { inner, files }
    }
}

impl Source for SnapshotFiles {
    fn discover(&self, _root: &str) -> Result<Vec<FileFingerprint>> {
        Ok(self.files.clone())
    }

    fn row_groups(&self, file: &FileFingerprint) -> Result<Vec<bellwether_types::RowGroupMeta>> {
        self.inner.row_groups(file)
    }

    fn read_columns(
        &self,
        rg: &bellwether_types::RowGroupMeta,
        columns: &[String],
    ) -> Result<arrow::array::RecordBatch> {
        self.inner.read_columns(rg, columns)
    }

    fn schema(&self, root: &str) -> Result<SchemaRef> {
        self.inner.schema(root)
    }
}

/// Lists the folder now and compares it with the pinned manifest's files.
///
/// Cheap by design, whatever the fingerprint policy: each data file is
/// listed and `stat`ed (path, size, and etag or mtime), never read. A size or
/// etag/mtime that differs from the manifest's is a changed fingerprint by
/// definition, so no content hash is needed to report it. Only files the
/// index does not track are fingerprinted (under the index's policy), because
/// `why` reads their footers. Under the `full` policy this means the folder
/// check never re-hashes the lake; an in-place patch that keeps a file's
/// size and mtime is caught where `full` promises it, when the planner
/// verifies each file it plans against its full-content fingerprint before
/// reading it.
pub(crate) fn folder_changes(
    root: &str,
    pinned: &Pinned,
    format: SourceFormat,
    exclude: &[String],
) -> Result<FolderChanges> {
    let tracked: std::collections::BTreeMap<&str, &FileFingerprint> = pinned
        .manifest()
        .rowgroup_space
        .files
        .iter()
        .map(|f| (f.fingerprint.path.as_str(), &f.fingerprint))
        .collect();
    let policy = pinned.policy()?;
    let text = format.text();
    let fs = LocalFs;
    let mut found = std::collections::BTreeSet::new();
    let mut changes = FolderChanges::default();
    let mut paths: Vec<String> = fs
        .list(root)?
        .into_iter()
        .filter(|p| {
            !exclude.iter().any(|d| {
                p.strip_prefix(d.as_str())
                    .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
            })
        })
        .filter(|p| match &text {
            None => Path::new(p)
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e.eq_ignore_ascii_case("parquet")),
            Some(t) => t.candidate(p),
        })
        .collect();
    paths.sort();
    for path in paths {
        match tracked.get(path.as_str()) {
            None => {
                // A `.json` file counts only when its content is JSON lines,
                // as discovery decides.
                if let Some(t) = &text {
                    if !t.accepts(&fs, &path)? {
                        continue;
                    }
                }
                changes
                    .untracked
                    .push(fingerprint_with_policy(&fs, &path, policy)?);
            }
            Some(t) => {
                let meta = fs.head(&path)?;
                if meta.size != t.size || meta.etag != t.etag {
                    changes.changed.push(path.clone());
                }
            }
        }
        found.insert(path);
    }
    changes.missing = tracked
        .keys()
        .filter(|p| !found.contains(**p))
        .map(ToString::to_string)
        .collect();
    Ok(changes)
}

/// The one chunk size an index's text files record, if any. Files recorded
/// at different sizes cannot share one source; that is refused.
fn recorded_chunk_size(m: &Manifest) -> Result<Option<u64>> {
    let mut sizes: Vec<u64> = m
        .rowgroup_space
        .files
        .iter()
        .filter_map(|f| f.chunk_size)
        .collect();
    sizes.sort_unstable();
    sizes.dedup();
    match sizes.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some(*one)),
        many => Err(Error::Source(format!(
            "the index records text files at {} chunk sizes ({many:?}); the facade reads one \
             chunk size per index, so rebuild the index at one size",
            many.len()
        ))),
    }
}

/// Time from the commit that wrote `m` to `now_nanos` (zero if the commit's
/// clock ran ahead).
pub(crate) fn commit_age(m: &Manifest, now_nanos: i64) -> Result<Duration> {
    let committed = if m.committed_at.is_empty() {
        &m.created_at
    } else {
        &m.committed_at
    };
    let at = parse_rfc3339(committed)?;
    let at_nanos = at
        .timestamp_nanos_opt()
        .ok_or_else(|| Error::Other(format!("commit time {committed} is out of range")))?;
    Ok(Duration::from_nanos(
        u64::try_from(now_nanos.saturating_sub(at_nanos)).unwrap_or(0),
    ))
}

impl Engine {
    /// The build options every index commit uses (`index` and the apply
    /// stage of `fix` alike), from this engine's options: the source format
    /// (a CSV dialect included), chunk size, dataset schema, fingerprint
    /// policy, and partitioning (hive or a path template), so whichever verb
    /// builds an index first records the same `manifest.source`; plus
    /// `sidecars` to declare and the index directory to exclude.
    pub(crate) fn build_options(&self, sidecars: Vec<SidecarDef>) -> BuildOptions {
        let mut options = BuildOptions::new(self.format().kind());
        options.chunk_size = self.options.chunk_size;
        options.schema.clone_from(&self.options.schema);
        options.fingerprint_policy = self.options.fingerprint_policy;
        options.partitioning.clone_from(&self.options.partitioning);
        options.sidecars = sidecars;
        options.exclude = self.excluded();
        options
    }

    /// Directories under the data root whose files are never data: the
    /// index location, when it is local and lies inside the root (its own
    /// `.json` files would otherwise pass for JSON lines). Spelled as the
    /// root's listed files are: the canonical root, then the index's path
    /// below it.
    pub(crate) fn excluded(&self) -> Vec<String> {
        let Some(dir) = &self.local_index else {
            return Vec::new();
        };
        // The index may not exist yet: resolve its nearest existing
        // ancestor, then append the rest.
        let mut existing = dir.as_path();
        let mut rest = Vec::new();
        let resolved = loop {
            if let Ok(c) = std::fs::canonicalize(existing) {
                break rest.iter().rev().fold(c, |p: PathBuf, r| p.join(r));
            }
            match (existing.parent(), existing.file_name()) {
                (Some(parent), Some(name)) => {
                    rest.push(name.to_os_string());
                    existing = parent;
                }
                _ => return Vec::new(),
            }
        };
        match resolved.strip_prefix(&self.root) {
            Ok(below) if !below.as_os_str().is_empty() => {
                vec![resolved.to_string_lossy().into_owned()]
            }
            _ => Vec::new(),
        }
    }
}

/// The index location as a local directory, or `None` for an object store.
fn local_path(index: &str) -> Result<Option<PathBuf>> {
    let path = match index.split_once("://") {
        None => index,
        Some(("file", rest)) => rest,
        Some(_) => return Ok(None),
    };
    std::path::absolute(path)
        .map(Some)
        .map_err(|e| Error::Store(format!("cannot resolve the index location {index:?} ({e})")))
}

fn join_path(root: &str, name: &str) -> String {
    Path::new(root).join(name).to_string_lossy().into_owned()
}

/// One column of a schema file.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaColumn {
    name: String,
    #[serde(rename = "type")]
    data_type: String,
    #[serde(default = "nullable_default")]
    nullable: bool,
}

fn nullable_default() -> bool {
    true
}

/// Parses a dataset schema for [`EngineOptions::schema`] from JSON: a list
/// of `{"name": ..., "type": ..., "nullable": ...}` objects in column order,
/// each `type` spelled as Arrow prints it and as the manifest records it
/// (`"Int64"`, `"Decimal128(12, 4)"`, `"Timestamp(Microsecond, Some(\"UTC\"))"`).
/// `nullable` defaults to true.
pub fn schema_from_json(json: &str) -> Result<SchemaRef> {
    use std::str::FromStr;
    let cols: Vec<SchemaColumn> = serde_json::from_str(json).map_err(|e| {
        Error::Other(format!(
            "the schema is not a JSON list of {{\"name\", \"type\", \"nullable\"}} objects ({e})"
        ))
    })?;
    let mut seen = std::collections::BTreeSet::new();
    let fields = cols
        .into_iter()
        .map(|c| {
            if !seen.insert(c.name.clone()) {
                return Err(Error::Other(format!(
                    "the schema lists column {:?} twice",
                    c.name
                )));
            }
            let dt = arrow::datatypes::DataType::from_str(&c.data_type).map_err(|e| {
                Error::Other(format!(
                    "column {:?} has type {:?}, which Arrow cannot parse ({e}); spell it as \
                     Arrow prints it, for example \"Decimal128(12, 4)\"",
                    c.name, c.data_type
                ))
            })?;
            Ok(arrow::datatypes::Field::new(c.name, dt, c.nullable))
        })
        .collect::<Result<Vec<_>>>()?;
    if fields.is_empty() {
        return Err(Error::Other("the schema lists no columns".into()));
    }
    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

/// A value that does not fit the dataset type usually means the dataset
/// schema was inferred from a file that does not represent every file (for
/// example the first file's decimal scale); the error then says how to
/// supply the schema.
pub(crate) fn schema_hint(e: Error) -> Error {
    const HINT: &str = "; if files differ in decimal scale or time unit, pass the dataset \
                        schema with --schema <file> (EngineOptions::schema)";
    let needs = |m: &str| m.contains("not representable in dataset type");
    match e {
        Error::Source(m) if needs(&m) => Error::Source(m + HINT),
        Error::KeySpec(m) if needs(&m) => Error::KeySpec(m + HINT),
        Error::Other(m) if needs(&m) => Error::Other(m + HINT),
        other => other,
    }
}

/// Runs `f`, turning a panic into an error that names the issue tracker, so
/// no panic reaches a caller of the facade.
pub(crate) fn guard<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(payload) => {
            let what = payload
                .downcast_ref::<&str>()
                .map(ToString::to_string)
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            Err(Error::Other(format!(
                "internal error ({what}); this is a bug, please report it at {ISSUE_TRACKER}"
            )))
        }
    }
}
