//! The synchronous core: one call in, one reply out. The rmcp handler is a
//! thin wrapper over [`Server::call`], and the tests call it directly.
//!
//! Every tool is one facade call with no logic of its own: arguments are
//! validated against the tool's closed schema, deserialized, passed to the
//! facade, and the facade's typed result is returned with its envelope.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use bellwether::types::{Error, ProposalBody};
use bellwether::{
    Engine, EngineOptions, FixOutcome, FixRequest, FixResult, KeyFile, ProposeResult, TextQuery,
    WhyRequest, DEFAULT_MAX_CANDIDATES,
};
use serde::Serialize;
use serde_json::{json, Value};

use crate::budget::{Budget, BudgetState, Exceeded, Meter};
use crate::envelope::{cites_of, Chain, Envelope, Status};
use crate::jobs::{job_id, Job, JobInfo, JobState, Jobs};
use crate::refusal::{self, Refusal, RefusalCode};
use crate::tools::{
    self, ApplyReceipt, ApplyRefresh, EmitEngine, EmitResidual, JobRef, JobsStart, KeyArg,
    Membership, ObserveAggregate, ObserveAsof, ObserveCardinality, ObserveCount,
    ObserveJoinOverlap, ObserveJoinVerify, ObserveLocate, ObserveOverlap, ObservePlanFromSql,
    ObserveProfile, ObserveSemijoin, ObserveText, ObserveWhy, ProposeKey, ProposeLayout,
    ProposeRewrite, ScoreAdvise, ScoreRecord, ScoreVerify, ScoreWhy, Tier,
};

/// How the server is configured at startup. Nothing is read from the
/// environment.
#[derive(Clone, Debug, Default)]
pub struct Config {
    /// The data folder the server is bound to.
    pub root: String,
    /// How the engine opens it (index location, format, clock).
    pub engine: EngineOptions,
    /// The per-session budget.
    pub budget: Budget,
    /// The join key the semi-join tools plan with (B3; the operator's
    /// `--join`): `<right>.<cols>` or `join:<left>.<cols>=<right>.<cols>`.
    /// `None`: those tools are refused.
    pub join: Option<String>,
    /// Serve `root` as a sandboxed workspace directory holding multiple data
    /// folders: a tool call's optional `dataset` argument may name a relative
    /// folder under the canonicalized `root`, consumer plans and SQL plans
    /// auto-bind sibling folders under the workspace, and semi-join keys may
    /// join folders within the workspace.
    pub workspace: bool,
}

/// One tool call's reply.
#[derive(Clone, Debug, PartialEq)]
pub struct Reply {
    /// The text content: the facade's typed result exactly as
    /// `serde_json::to_string` writes it (the CLI's `--json` line without
    /// its newline), or the refusal when the facade returned an error, or
    /// the job when the call is running as one.
    pub text: String,
    /// The structured content.
    pub envelope: Envelope,
}

impl Reply {
    /// Whether the call was refused (the MCP result's `isError`).
    #[must_use]
    pub fn is_error(&self) -> bool {
        self.envelope.status == Status::Refused
    }
}

/// What a facade call produced.
enum Done {
    /// A typed result, with a refusal when the facade refused in-band.
    Result {
        text: String,
        value: Value,
        refusal: Option<Refusal>,
        engine: Option<Box<Engine>>,
    },
    /// Refused before or by the facade with an error.
    Failed(Refusal),
}

fn done<T: Serialize>(r: &T, refusal: Option<Refusal>, engine: Option<Engine>) -> Done {
    match (serde_json::to_string(r), serde_json::to_value(r)) {
        (Ok(text), Ok(value)) => Done::Result {
            text,
            value,
            refusal,
            engine: engine.map(Box::new),
        },
        (Err(e), _) | (_, Err(e)) => Done::Failed(refusal::from_error(&Error::Serde(e))),
    }
}

fn failed(e: &Error) -> Done {
    Done::Failed(refusal::from_error(e))
}

struct Inner {
    engine: Engine,
    /// The data folder and how the engine opens it, for a lookup pinned to
    /// a committed manifest (`at_manifest`).
    root: String,
    options: EngineOptions,
    explicit_datasets: std::collections::BTreeSet<String>,
    join: Option<String>,
    workspace: bool,
    budget: Meter,
    jobs: Mutex<Jobs>,
}

/// The MCP server's core over one data folder.
#[derive(Clone)]
pub struct Server {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server")
            .field("engine", &self.inner.engine)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The right side of an operator's join key (`--join`), `<right>.<cols>`:
/// the key's right side when it is a whole key, `join:...`, else the value.
fn join_side_of(j: &str) -> bellwether::types::Result<String> {
    if !j.trim_start().starts_with("join:") {
        bellwether::spec::parse_join_side(j)?;
        return Ok(j.trim().to_string());
    }
    match bellwether::spec::parse(j)? {
        bellwether::types::KeySpec::Join {
            right,
            right_columns,
            ..
        } => Ok(format!("{right}.{}", right_columns.join(","))),
        other => Err(bellwether::types::Error::KeySpec(format!(
            "{} is not a join key",
            other.name()
        ))),
    }
}

impl Server {
    /// Opens the engine over `config.root`. Opening writes nothing. The
    /// engine reads no right dataset but the one `config.join` names (B3,
    /// `EngineOptions::join_scope`): a join key a tool argument names with
    /// another right side is refused before any folder is read.
    pub fn new(config: &Config) -> bellwether::types::Result<Self> {
        let explicit_datasets: std::collections::BTreeSet<String> =
            config.engine.datasets.keys().cloned().collect();
        let mut options = EngineOptions {
            join_scope: match (&config.join, config.workspace) {
                (Some(j), _) => bellwether::JoinScope::Only(join_side_of(j)?),
                (None, true) => bellwether::JoinScope::Any,
                (None, false) => bellwether::JoinScope::Unserved,
            },
            ..config.engine.clone()
        };
        let mut engine = Engine::open(&config.root, options.clone())?;
        if config.workspace {
            discover_workspace_datasets(std::path::Path::new(engine.root()), &mut options.datasets);
            engine = Engine::open(&config.root, options.clone())?;
        }
        Ok(Self {
            inner: Arc::new(Inner {
                engine,
                root: config.root.clone(),
                options,
                explicit_datasets,
                join: config.join.clone(),
                workspace: config.workspace,
                budget: Meter::new(config.budget),
                jobs: Mutex::new(Jobs::new()),
            }),
        })
    }

    /// The engine (for tests that compare against the facade directly).
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.inner.engine
    }

    /// Resolves the engine for a tool call's optional `dataset` argument and
    /// optional `at_manifest` sequence.
    ///
    /// Returns `Ok(None)` when both `dataset` and `at_manifest` are absent (or
    /// `dataset` is `"main"` without `at_manifest`) and not in workspace mode,
    /// so the caller can use `&self.inner.engine` without opening a new engine.
    fn engine_for(
        &self,
        dataset: Option<&str>,
        at_manifest: Option<u64>,
    ) -> bellwether::types::Result<Option<Engine>> {
        let Some(name) = dataset else {
            if self.inner.workspace && !self.inner.engine.is_table() {
                let mut opts = self.inner.options.clone();
                opts.at_manifest = at_manifest;
                let ws_root = std::path::Path::new(self.inner.engine.root());
                discover_workspace_datasets(ws_root, &mut opts.datasets);
                return Engine::open(&self.inner.root, opts).map(Some);
            }
            return at_manifest
                .map(|seq| {
                    Engine::open(
                        &self.inner.root,
                        EngineOptions {
                            at_manifest: Some(seq),
                            ..self.inner.options.clone()
                        },
                    )
                })
                .transpose();
        };
        if name == "main" {
            return self.engine_for(None, at_manifest);
        }
        if self.inner.explicit_datasets.contains(name) {
            if let Some(bound_root) = self.inner.options.datasets.get(name) {
                let mut opts = self.inner.options.clone();
                opts.at_manifest = at_manifest;
                opts.index = None;
                return Engine::open(bound_root, opts).map(Some);
            }
        }
        if !self.inner.workspace {
            return Err(Error::InvalidArgument {
                field: "dataset",
                reason: "the server is bound to one folder; start it with --workspace (or \
                         --dataset <name>=<path>) to target another folder"
                    .into(),
            });
        }
        let resolved = self.resolve_workspace_dataset(name)?;
        let mut opts = self.inner.options.clone();
        opts.at_manifest = at_manifest;
        opts.index = None;
        let ws_root = std::path::Path::new(self.inner.engine.root());
        discover_workspace_datasets(ws_root, &mut opts.datasets);
        if let Some(file_name) = resolved.file_name().and_then(|f| f.to_str()) {
            opts.datasets
                .entry(file_name.to_string())
                .or_insert_with(|| resolved.to_string_lossy().into_owned());
        }
        opts.datasets
            .entry(name.trim().to_string())
            .or_insert_with(|| resolved.to_string_lossy().into_owned());
        Engine::open(&resolved.to_string_lossy(), opts).map(Some)
    }

    /// Validates and resolves `name` as a relative directory inside the
    /// canonicalized workspace root. Refuses empty names, URLs, absolute paths,
    /// `..` traversal components, non-directories, and symlinks resolving
    /// outside the workspace root.
    fn resolve_workspace_dataset(
        &self,
        raw: &str,
    ) -> bellwether::types::Result<std::path::PathBuf> {
        let name = raw.trim();
        if name.is_empty() {
            return Err(Error::InvalidArgument {
                field: "dataset",
                reason: "`dataset` must not be empty; pass a relative folder name under the \
                         workspace root (or omit `dataset` for the workspace root)"
                    .into(),
            });
        }
        if name.contains("://") {
            return Err(Error::InvalidArgument {
                field: "dataset",
                reason: format!(
                    "`dataset` {raw:?} is a URL; in `--workspace` mode `dataset` must be a \
                     relative folder path inside the workspace"
                ),
            });
        }
        let rel = std::path::Path::new(name);
        if rel.is_absolute() {
            return Err(Error::InvalidArgument {
                field: "dataset",
                reason: format!(
                    "`dataset` {raw:?} is an absolute path; pass a relative folder path inside \
                     the workspace"
                ),
            });
        }
        for comp in rel.components() {
            match comp {
                std::path::Component::Normal(_) | std::path::Component::CurDir => {}
                std::path::Component::ParentDir
                | std::path::Component::RootDir
                | std::path::Component::Prefix(_) => {
                    return Err(Error::InvalidArgument {
                        field: "dataset",
                        reason: format!(
                            "`dataset` {raw:?} escapes the workspace (`..` or root components \
                             are not allowed); pass a relative folder path inside the workspace"
                        ),
                    });
                }
            }
        }
        let ws_root = std::path::Path::new(self.inner.engine.root());
        let candidate = ws_root.join(rel);
        let canonical = std::fs::canonicalize(&candidate).map_err(|e| Error::InvalidArgument {
            field: "dataset",
            reason: format!(
                "cannot open dataset {raw:?} under workspace {} ({e}); pass an existing \
                 directory inside the workspace",
                ws_root.display()
            ),
        })?;
        if !canonical.starts_with(ws_root) {
            return Err(Error::InvalidArgument {
                field: "dataset",
                reason: format!(
                    "`dataset` {raw:?} resolves outside the workspace root {}; symlinks and \
                     paths outside the workspace are refused",
                    ws_root.display()
                ),
            });
        }
        if !canonical.is_dir() {
            return Err(Error::InvalidArgument {
                field: "dataset",
                reason: format!(
                    "`dataset` {raw:?} is not a directory; pass a data folder inside the \
                     workspace"
                ),
            });
        }
        Ok(canonical)
    }
    /// The join key the operator started the server with (`--join`), or
    /// the refusal saying a semi-join tool needs one.
    fn join(&self) -> bellwether::types::Result<&str> {
        self.inner
            .join
            .as_deref()
            .ok_or_else(|| bellwether::types::Error::InvalidArgument {
                field: "join",
                reason: "the server was started without a join key, so it plans no semi-join; \
                         ask the human to start it with --join <right>.<columns> (the right \
                         dataset is a folder beside the data root)"
                    .into(),
            })
    }

    /// The right side of the operator's join key, as `why` takes it:
    /// `<right>.<cols>`.
    fn join_side(&self) -> bellwether::types::Result<String> {
        join_side_of(self.join()?)
    }

    /// The session budget now.
    #[must_use]
    pub fn budget(&self) -> BudgetState {
        self.inner.budget.state()
    }

    /// Calls `tool` with `args`. Never panics into the caller: the facade
    /// turns internal panics into errors, and every error is a refusal.
    #[must_use]
    pub fn call(&self, tool: &str, args: Value) -> Reply {
        let Some(def) = tools::find(tool) else {
            return self.refused(
                tool,
                Tier::Jobs,
                Refusal::new(
                    RefusalCode::UnknownTool,
                    format!("no tool is named {tool:?}"),
                    json!({ "tool": tool }),
                ),
            );
        };
        if let Err(errors) = tools::validate(&def.input, &args) {
            return self.refused(
                tool,
                def.tier,
                schema_refusal(tool, &def.input, &args, &errors),
            );
        }
        match tool {
            "jobs.start" => match serde_json::from_value::<JobsStart>(args) {
                Ok(s) => self.start(&s.tool, Value::Object(s.arguments), true),
                Err(e) => self.invalid(tool, def.tier, &e),
            },
            "jobs.status" | "jobs.result" => match serde_json::from_value::<JobRef>(args) {
                Ok(r) => self.job(tool, &r.job),
                Err(e) => self.invalid(tool, def.tier, &e),
            },
            _ if runs_as_job(tool, &args) => self.start(tool, args, false),
            _ => self.run(tool, def.tier, args, None),
        }
    }

    /// Runs a tool now: admission (the budget check, and for a charged call
    /// the reservation, `crate::budget`), the facade call, the charge, the
    /// envelope. `job` is the job this run is for, whose progress says so
    /// while it waits for another charged call to settle.
    fn run(&self, tool: &str, tier: Tier, args: Value, job: Option<&str>) -> Reply {
        let mark_queued = |queued: bool| {
            if let Some(id) = job {
                if let Some(j) = lock(&self.inner.jobs).get_mut(id) {
                    j.queued = queued;
                }
            }
        };
        let waited = std::cell::Cell::new(false);
        let admitted = self.inner.budget.admit(charged(tier, &args), || {
            waited.set(true);
            mark_queued(true);
        });
        if waited.get() {
            mark_queued(false);
        }
        let reservation = match admitted {
            Ok(r) => r,
            Err(Exceeded {
                dimension,
                spent,
                limit,
            }) => {
                return self.refused(
                    tool,
                    tier,
                    Refusal::new(
                        RefusalCode::BudgetExceeded,
                        format!("this session spent its {dimension} budget ({spent} of {limit})"),
                        json!({ "dimension": dimension, "spent": spent, "limit": limit }),
                    ),
                )
            }
        };
        let started = Instant::now();
        let outcome = self.dispatch(tool, args);
        let elapsed = started.elapsed();
        match outcome {
            Done::Result {
                text,
                value,
                refusal,
                engine,
            } => {
                reservation.settle(Some(&value), elapsed);
                let (cites, receipts) = cites_of(&value);
                let receipt_engine = engine.as_deref().unwrap_or(&self.inner.engine);
                let chains = receipts
                    .iter()
                    .filter_map(|h| receipt_engine.receipt(h).ok())
                    .map(|r| Chain::from(&r))
                    .collect();
                let status = if refusal.is_some() {
                    Status::Refused
                } else {
                    Status::Ok
                };
                Reply {
                    text,
                    envelope: Envelope {
                        tool: tool.to_string(),
                        tier,
                        status,
                        cites,
                        chains,
                        refusal,
                        job: None,
                        budget: self.budget(),
                    },
                }
            }
            Done::Failed(r) => {
                reservation.settle(None, elapsed);
                self.refused(tool, tier, r)
            }
        }
    }

    /// The one facade call behind each tool.
    #[allow(clippy::too_many_lines)]
    fn dispatch(&self, tool: &str, args: Value) -> Done {
        let default_engine = &self.inner.engine;
        macro_rules! parse {
            ($t:ty) => {
                match serde_json::from_value::<$t>(args) {
                    Ok(a) => a,
                    Err(err) => return Done::Failed(invalid_refusal(tool, &err)),
                }
            };
        }
        macro_rules! resolve_engine {
            ($ds:expr, $at:expr) => {
                match self.engine_for($ds, $at) {
                    Ok(opt) => opt,
                    Err(err) => return failed(&err),
                }
            };
        }
        match tool {
            "observe.why" => {
                let a = parse!(ObserveWhy);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                match e.why(&WhyRequest {
                    columns: a.columns,
                    sketches_only: a.sketches_only,
                    ..WhyRequest::default()
                }) {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.cardinality" => {
                let a = parse!(ObserveCardinality);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                match e.cardinality(&bellwether::CardinalityRequest {
                    items: a.items,
                    transient: a.transient,
                }) {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.overlap" => {
                let a = parse!(ObserveOverlap);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                match e.overlap(&a.left, &a.right, None) {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.profile" => {
                let a = parse!(ObserveProfile);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                match e.findings() {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.locate" => {
                let a = parse!(ObserveLocate);
                let keys = match resolve_keys(a.keys, a.key_columns, a.key_values) {
                    Ok(k) => k,
                    Err(err) => return failed(&err),
                };
                let target = resolve_engine!(a.dataset.as_deref(), a.at_manifest);
                let engine = target.as_ref().unwrap_or(default_engine);
                let result = match (&a.columns, a.contains) {
                    (Some(_), Some(_)) => Err(bellwether::types::Error::InvalidArgument {
                        field: "columns",
                        reason: "a membership lookup (contains) plans row groups without a \
                                 projection; drop columns, or drop contains for a point lookup \
                                 with column bytes"
                            .into(),
                    }),
                    (None, Some(m)) => engine.locate_contains(&keys, m == Membership::All),
                    (Some(columns), None) => engine.locate_columns(&keys, None, columns),
                    (None, None) => engine.locate(&keys),
                };
                match result
                    .and_then(|r| with_emit(engine, r, a.emit, a.ordered, a.residual, a.relocate))
                {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.count" => {
                let a = parse!(ObserveCount);
                let keys = match resolve_keys(a.keys, a.key_columns, a.key_values) {
                    Ok(k) => k,
                    Err(err) => return failed(&err),
                };
                let target = resolve_engine!(a.dataset.as_deref(), a.at_manifest);
                match target.as_ref().unwrap_or(default_engine).count(&keys) {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.aggregate" => {
                let a = parse!(ObserveAggregate);
                let keys = match resolve_keys(a.keys, a.key_columns, a.key_values) {
                    Ok(k) => k,
                    Err(err) => return failed(&err),
                };
                let target = resolve_engine!(a.dataset.as_deref(), a.at_manifest);
                let functions = a.functions.unwrap_or_else(tools::default_functions);
                match target
                    .as_ref()
                    .unwrap_or(default_engine)
                    .aggregate(&keys, &a.value, &functions)
                {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.asof" => {
                let a = parse!(ObserveAsof);
                let keys = match resolve_keys(a.keys, a.key_columns, a.key_values) {
                    Ok(k) => k,
                    Err(err) => return failed(&err),
                };
                let target = resolve_engine!(a.dataset.as_deref(), a.at_manifest);
                let engine = target.as_ref().unwrap_or(default_engine);
                let clocks = match (&a.valid_by, &a.known_by) {
                    (Some(v), Some(k)) => Ok(Some((v.as_str(), k.as_str()))),
                    (None, None) => Ok(None),
                    (v, _) => Err(Error::InvalidArgument {
                        field: if v.is_some() { "known_by" } else { "valid_by" },
                        reason: "a bitemporal key is picked by both of its clock columns: give \
                                 `valid_by` and `known_by` together"
                            .into(),
                    }),
                };
                let bitemporal = a.valid.is_some() || a.known.is_some();
                let r = match (&a.at, bitemporal, clocks) {
                    (_, _, Err(err)) => Err(err),
                    (Some(at), false, Ok(None)) if a.live => engine.asof_live(&keys, at),
                    (Some(at), false, Ok(None)) => engine.asof(&keys, at),
                    (Some(_), false, Ok(Some(_))) => Err(Error::InvalidArgument {
                        field: "valid_by",
                        reason: "`valid_by` and `known_by` pick the bitemporal key of a \
                                 bitemporal as-of: give `valid` and/or `known` with them, not \
                                 `at`"
                            .into(),
                    }),
                    (None, true, Ok(clocks)) => engine.asof_bitemporal(
                        &keys,
                        a.valid.as_deref(),
                        a.known.as_deref(),
                        clocks,
                        a.live,
                    ),
                    (Some(_), true, _) => Err(Error::InvalidArgument {
                        field: "at",
                        reason: "`at` is a temporal as-of; `valid` and `known` a bitemporal one: \
                                 give `at`, or `valid` and/or `known`, not both"
                            .into(),
                    }),
                    (None, false, _) => Err(Error::InvalidArgument {
                        field: "at",
                        reason: "an as-of needs an instant: give `at` (a temporal key), or \
                                 `valid` and/or `known` (a temporal2 key)"
                            .into(),
                    }),
                };
                match r
                    .and_then(|r| with_emit(engine, r, a.emit, a.ordered, a.residual, a.relocate))
                {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.semijoin" => {
                let a = parse!(ObserveSemijoin);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                match self
                    .join()
                    .and_then(|j| e.semijoin(j, &key_file(a.keys, a.key_columns)))
                {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.join_overlap" => {
                let a = parse!(ObserveJoinOverlap);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                match self.join().and_then(|j| e.join_overlap(j)) {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.join_verify" => {
                let a = parse!(ObserveJoinVerify);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                match e.join_verify(self.inner.join.as_deref()) {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.plan_from_sql" => {
                let a = parse!(ObservePlanFromSql);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                match e.plan_from_sql(&bellwether::PlanFromSqlRequest {
                    sql: a.sql,
                    main_table: a.main_table,
                    keys_from_run: false,
                    as_of: a.as_of,
                    validate: true,
                    cache: true,
                }) {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "observe.text" => {
                let a = parse!(ObserveText);
                let target = resolve_engine!(a.dataset.as_deref(), a.at_manifest);
                let query = TextQuery {
                    column: a.column,
                    op: a.op,
                    value: a.value,
                };
                match target
                    .as_ref()
                    .unwrap_or(default_engine)
                    .locate_text(&query)
                {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "score.why" => {
                let a = parse!(ScoreWhy);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                if a.keys.is_none()
                    && a.key_values.is_none()
                    && a.workload.is_none()
                    && a.text.is_empty()
                {
                    return Done::Failed(Refusal::new(
                        RefusalCode::NoWorkload,
                        "score.why replays a workload (keys, key_values, workload, or text \
                         lookups); without one, call observe.why",
                        json!({}),
                    ));
                }
                let keys = match (a.keys.is_some(), a.key_values.is_some()) {
                    (false, false) => None,
                    _ => match resolve_keys(a.keys, a.key_columns.clone(), a.key_values) {
                        Ok(k) => Some(k),
                        Err(err) => return failed(&err),
                    },
                };
                let tombstone = match a
                    .tombstone
                    .as_deref()
                    .map(bellwether::spec::parse_tombstone)
                {
                    Some(Err(err)) => return failed(&err),
                    Some(Ok(t)) => Some(t),
                    None => None,
                };
                let request = WhyRequest {
                    tombstone,
                    more_keys: a
                        .more_keys
                        .into_iter()
                        .map(|k| key_file(k, a.key_columns.clone()))
                        .collect(),
                    keys,
                    bench_workload: None,
                    workload: a.workload,
                    mode: match (a.contains, a.join) {
                        (Some(_), true) => {
                            return failed(&bellwether::types::Error::InvalidArgument {
                                field: "join",
                                reason: "a run is a list membership (contains) or a semi-join \
                                         (join), not both; drop one"
                                    .into(),
                            })
                        }
                        (Some(Membership::Any), false) => Some(bellwether::LookupMode::ContainsAny),
                        (Some(Membership::All), false) => Some(bellwether::LookupMode::ContainsAll),
                        (None, true) => Some(bellwether::LookupMode::SemiJoin),
                        (None, false) => None,
                    },
                    latest_by: a.latest_by,
                    at: a.at,
                    lookback: None,
                    valid_by: a.valid_by,
                    known_by: a.known_by,
                    valid: a.valid,
                    known: a.known,
                    window: None,
                    ts_column: None,
                    columns: a.columns,
                    max_candidates: a
                        .max_candidates
                        .map_or(DEFAULT_MAX_CANDIDATES, |n| n as usize),
                    gates: a.gates,
                    sketches_only: false,
                    text: a.text.into_iter().map(Into::into).collect(),
                    join: if a.join {
                        match self.join_side() {
                            Ok(j) => Some(j),
                            Err(err) => return failed(&err),
                        }
                    } else {
                        None
                    },
                    aggregate: a.aggregate.map(|g| bellwether::AggregateQuery {
                        value: g.value,
                        functions: g.functions.unwrap_or_else(tools::default_functions),
                    }),
                    plan: a.plan.map(std::path::PathBuf::from),
                    consumer_plan: None,
                    datasets: a.datasets,
                };
                match e.why(&request) {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "score.verify" => {
                let a = parse!(ScoreVerify);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                // A tool argument never names a database to connect to: a
                // table is a data root the human gives at start (threat
                // model).
                if let Some(r) = &a.rewrite {
                    if [&r.before, &r.after]
                        .iter()
                        .any(|p| bellwether::is_sql_root(p))
                    {
                        return failed(&bellwether::types::Error::InvalidArgument {
                            field: "rewrite",
                            reason: "a rewrite's outputs are folders of files here; a table \
                                     (sql://) is read only as the data root the human starts \
                                     the server with"
                                .into(),
                        });
                    }
                }
                let request = FixRequest {
                    proposal: Some(a.proposal),
                    certificate: None,
                    workload: a.workload,
                    rewrite: a.rewrite.map(|r| bellwether::RewriteOutputs {
                        before: r.before,
                        after: r.after,
                        key: r.key,
                        latest_by: r.latest_by,
                        ignore: r.ignore,
                        totals: r.totals,
                        group_by: r.group_by,
                    }),
                    verify_only: true,
                    allow_no_gain: false,
                    engine: a.engine.map(|c| bellwether::EngineCompare {
                        latest_by: c.latest_by,
                        ignore: c.ignore,
                        totals: c.totals,
                        group_by: c.group_by,
                    }),
                };
                fix_done(e.fix(&request), target)
            }
            "score.advise" => {
                let a = parse!(ScoreAdvise);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                let mut workloads = a.workloads;
                if let Some(w) = a.workload {
                    if !workloads.contains(&w) {
                        workloads.insert(0, w);
                    }
                }
                let mut from = Vec::new();
                if let Some(k) = a.keys {
                    from.push(key_file(k, a.key_columns.clone()));
                }
                for k in a.more_keys {
                    from.push(key_file(k, a.key_columns.clone()));
                }
                if workloads.is_empty() && from.is_empty() {
                    return Done::Failed(Refusal::new(
                        RefusalCode::NoWorkload,
                        "score.advise needs a workload (`workload`, `workloads`, or `keys`)",
                        json!({ "field": "workload" }),
                    ));
                }
                let req = bellwether::AdviseRequest {
                    workloads,
                    from,
                    all_workloads: false,
                    max_keys: a
                        .max_keys
                        .map_or(bellwether::DEFAULT_MAX_KEYS, |n| n as usize),
                    max_index_bytes: a.max_index_bytes,
                    delta: a.delta.unwrap_or(bellwether::DEFAULT_DELTA),
                    apply: false,
                    gates: a.gates,
                };
                match e.advise(&req) {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "score.record" => {
                let a = parse!(ScoreRecord);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                match e.record_from_engine(&a.keys_query, a.key_columns.as_deref()) {
                    Ok(r) => done(&r, None, target),
                    Err(err) => failed(&err),
                }
            }
            "propose.key" => {
                let a = parse!(ProposeKey);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                // Each key is read as `index --key` reads it, so a sketch
                // reaches the facade's typed refusal.
                let mut declared = Vec::with_capacity(1 + a.sidecars.len());
                for arg in std::iter::once(a.spec).chain(a.sidecars) {
                    declared.push(match arg {
                        KeyArg::Short(s) => match bellwether::spec::parse_declaration(&s) {
                            Ok(d) => d,
                            Err(err) => return failed(&err),
                        },
                        KeyArg::Spec(spec) => bellwether::spec::Declaration {
                            key: Some(spec),
                            sketch: None,
                        },
                    });
                }
                propose_done(
                    if a.dry_run {
                        e.propose_key_dry_run(&declared, &a.evidence, &a.by)
                    } else {
                        e.propose_key(&declared, &a.evidence, &a.by)
                    },
                    target,
                )
            }
            "propose.layout" => {
                let a = parse!(ProposeLayout);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                propose(
                    target,
                    default_engine,
                    ProposalBody::Layout {
                        cluster_by: a.cluster_by,
                    },
                    &a.evidence,
                    &a.by,
                    a.dry_run,
                )
            }
            "propose.rewrite" => {
                let a = parse!(ProposeRewrite);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                propose(
                    target,
                    default_engine,
                    ProposalBody::Rewrite { shape: a.shape },
                    &a.evidence,
                    &a.by,
                    a.dry_run,
                )
            }
            "apply.receipt" => {
                let a = parse!(ApplyReceipt);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                if a.dry_run {
                    fix_done(e.apply_dry_run(&a.certificate), target)
                } else {
                    fix_done(
                        e.fix(&FixRequest {
                            certificate: Some(a.certificate),
                            ..FixRequest::default()
                        }),
                        target,
                    )
                }
            }
            "apply.refresh" => {
                let a = parse!(ApplyRefresh);
                let target = resolve_engine!(a.dataset.as_deref(), None);
                let e = target.as_ref().unwrap_or(default_engine);
                if a.dry_run {
                    match e.refresh_dry_run() {
                        Ok(r) => done(&r, None, target),
                        Err(err) => failed(&err),
                    }
                } else {
                    match e.index(&[]) {
                        Ok(r) => done(&r, None, target),
                        Err(err) => failed(&err),
                    }
                }
            }
            other => Done::Failed(Refusal::new(
                RefusalCode::UnknownTool,
                format!("no tool is named {other:?}"),
                json!({ "tool": other }),
            )),
        }
    }

    /// Starts `tool` as a job, or returns the job already started for the
    /// same request: a job id always names one run and one result. A direct
    /// call of a request whose job has finished returns that job's stored
    /// result (what `jobs.result` returns); `jobs.start` returns the job.
    fn start(&self, tool: &str, args: Value, via_jobs_start: bool) -> Reply {
        let shown = if via_jobs_start { "jobs.start" } else { tool };
        let Some(def) = tools::find(tool).filter(|d| d.tier != Tier::Jobs) else {
            return self.refused(
                shown,
                Tier::Jobs,
                Refusal::new(
                    RefusalCode::UnknownTool,
                    format!("no tool that can run as a job is named {tool:?}"),
                    json!({ "tool": tool }),
                ),
            );
        };
        if let Err(errors) = tools::validate(&def.input, &args) {
            return self.refused(
                shown,
                def.tier,
                schema_refusal(tool, &def.input, &args, &errors),
            );
        }
        let mut request = args.clone();
        if let Value::Object(m) = &mut request {
            // `wait` changes how the result is delivered, not what runs.
            m.remove("wait");
        }
        // The state the result depends on is part of the request: after a
        // commit or a data change the same arguments are a new job, so
        // retrying as a refusal's `next` line says runs again rather than
        // returning the stale result. An unreadable state is `null`; the
        // tool's own call then reports why.
        let target_ds = args.get("dataset").and_then(Value::as_str);
        let state = self
            .engine_for(target_ds, None)
            .ok()
            .and_then(|opt| opt.as_ref().unwrap_or(&self.inner.engine).state().ok())
            .and_then(|s| serde_json::to_value(s).ok())
            .unwrap_or(Value::Null);
        let id = job_id(tool, &json!({ "request": request, "state": state }));
        {
            let mut jobs = lock(&self.inner.jobs);
            if let Some(j) = jobs.get(&id) {
                if let (false, Some((_, reply))) = (via_jobs_start, &j.finished) {
                    return reply.clone();
                }
                let info = j.info(&id);
                drop(jobs);
                return self.job_reply(shown, def.tier, info);
            }
            jobs.insert(
                id.clone(),
                Job {
                    tool: tool.to_string(),
                    started: Instant::now(),
                    queued: false,
                    finished: None,
                },
            );
        }
        let server = self.clone();
        let (t, tier, jid) = (tool.to_string(), def.tier, id.clone());
        let spawned = std::thread::Builder::new()
            .name(format!("bellwether-job-{}", &id[..12]))
            .spawn(move || {
                let mut reply = server.run(&t, tier, args, Some(&jid));
                let mut jobs = lock(&server.inner.jobs);
                if let Some(j) = jobs.get_mut(&jid) {
                    let now = Instant::now();
                    j.finished = Some((now, reply.clone()));
                    reply.envelope.job = Some(j.info(&jid));
                    j.finished = Some((now, reply));
                }
            });
        if let Err(e) = spawned {
            lock(&self.inner.jobs).remove(&id);
            return self.refused(
                shown,
                def.tier,
                Refusal::new(
                    RefusalCode::EngineError,
                    format!("cannot start a job thread ({e})"),
                    json!({}),
                ),
            );
        }
        let info = lock(&self.inner.jobs)
            .get(&id)
            .map_or_else(|| running_info(&id, tool), |j| j.info(&id));
        self.job_reply(shown, def.tier, info)
    }

    /// `jobs.status` or `jobs.result`.
    fn job(&self, tool: &str, id: &str) -> Reply {
        let jobs = lock(&self.inner.jobs);
        let Some(j) = jobs.get(id) else {
            drop(jobs);
            return self.refused(
                tool,
                Tier::Jobs,
                Refusal::new(
                    RefusalCode::JobNotFound,
                    format!("no job {id} in this session"),
                    json!({ "job": id }),
                ),
            );
        };
        let info = j.info(id);
        if tool == "jobs.status" {
            drop(jobs);
            return self.job_reply(tool, Tier::Jobs, info);
        }
        if let Some((_, reply)) = &j.finished {
            return reply.clone();
        }
        drop(jobs);
        let mut r = self.refused(
            tool,
            Tier::Jobs,
            Refusal::new(
                RefusalCode::JobRunning,
                format!("job {id} has not finished"),
                json!({ "job": id }),
            ),
        );
        r.envelope.job = Some(info);
        r
    }

    fn job_reply(&self, tool: &str, tier: Tier, info: JobInfo) -> Reply {
        let status = match info.state {
            JobState::Running => Status::Running,
            JobState::Done => Status::Ok,
        };
        Reply {
            text: serde_json::to_string(&info).unwrap_or_default(),
            envelope: Envelope {
                tool: tool.to_string(),
                tier,
                status,
                cites: Vec::new(),
                chains: Vec::new(),
                refusal: None,
                job: Some(info),
                budget: self.budget(),
            },
        }
    }

    fn refused(&self, tool: &str, tier: Tier, r: Refusal) -> Reply {
        Reply {
            text: serde_json::to_string(&r).unwrap_or_default(),
            envelope: Envelope {
                tool: tool.to_string(),
                tier,
                status: Status::Refused,
                cites: Vec::new(),
                chains: Vec::new(),
                refusal: Some(r),
                job: None,
                budget: self.budget(),
            },
        }
    }

    fn invalid(&self, tool: &str, tier: Tier, e: &serde_json::Error) -> Reply {
        self.refused(tool, tier, invalid_refusal(tool, e))
    }

    /// The text of a resource: the facade's ledger view, serialized.
    pub fn resource(&self, uri: &str) -> Result<String, Refusal> {
        let e = &self.inner.engine;
        let text = match uri {
            crate::FINDINGS => e.findings().map(|r| serde_json::to_string(&r)),
            crate::RECEIPTS => e.receipts().map(|r| serde_json::to_string(&r)),
            crate::SCORECARD => e.scorecard().map(|r| serde_json::to_string(&r)),
            other => {
                return Err(Refusal::new(
                    RefusalCode::NotFound,
                    format!("no resource {other:?}"),
                    json!({ "uri": other }),
                ))
            }
        };
        match text {
            Ok(Ok(t)) => Ok(t),
            Ok(Err(s)) => Err(refusal::from_error(&Error::Serde(s))),
            Err(err) => Err(refusal::from_error(&err)),
        }
    }
}

fn running_info(id: &str, tool: &str) -> JobInfo {
    JobInfo {
        id: id.to_string(),
        tool: tool.to_string(),
        state: JobState::Running,
        progress: format!("{tool} started"),
    }
}

/// Whether a call is charged (`crate::budget`): a score or apply call that
/// is not a dry run, the calls that can write Replays or read rows to
/// verify. Charged calls are admitted one at a time.
fn charged(tier: Tier, args: &Value) -> bool {
    let dry_run = args
        .get("dry_run")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    matches!(tier, Tier::Score | Tier::Apply) && !dry_run
}

/// The refusal for arguments that do not match `tool`'s input schema. A
/// score, a cost, a measurement, or a verdict the caller tried to hand
/// over is refused by name (`caller_supplied_cost`): the README's
/// guarantee, the same whether the tool is called directly or started as a
/// job.
fn schema_refusal(tool: &str, schema: &Value, args: &Value, errors: &[String]) -> Refusal {
    if let Some(field) = supplied_cost(schema, args, errors) {
        return Refusal::new(
            RefusalCode::CallerSuppliedCost,
            format!(
                "{tool} was given `{field}`: a caller cannot supply a cost, a measurement, a \
                 replay, a verdict, or a certificate; the engine measures and verifies itself"
            ),
            json!({ "field": field, "errors": errors }),
        );
    }
    Refusal::new(
        RefusalCode::InvalidInput,
        format!("the arguments do not match {tool}'s input schema"),
        json!({ "errors": errors }),
    )
}

/// The cost-like field (`refusal::COST_FIELDS`) the caller supplied where
/// the schema takes none: named by a validation error, or, inside a tagged
/// union (whose error names no field), the first cost-like key below a
/// declared argument whose removal, with every other such key, makes the
/// arguments valid. `None` when the arguments are wrong for another reason.
fn supplied_cost(schema: &Value, args: &Value, errors: &[String]) -> Option<String> {
    if let Some(field) = refusal::caller_supplied(errors) {
        return Some(field);
    }
    let declared = schema.get("properties").and_then(Value::as_object)?;
    let mut stripped = args.clone();
    let mut found = None;
    if let Value::Object(top) = &mut stripped {
        for (k, v) in top.iter_mut() {
            if declared.contains_key(k) {
                strip_cost_fields(v, &mut found);
            }
        }
    }
    found.filter(|_| tools::validate(schema, &stripped).is_ok())
}

/// Removes every cost-like key at any depth of `v`, keeping the first
/// removed in `found`.
fn strip_cost_fields(v: &mut Value, found: &mut Option<String>) {
    match v {
        Value::Object(m) => {
            m.retain(|k, _| {
                let cost = refusal::COST_FIELDS.contains(&k.as_str());
                if cost && found.is_none() {
                    *found = Some(k.clone());
                }
                !cost
            });
            for x in m.values_mut() {
                strip_cost_fields(x, found);
            }
        }
        Value::Array(a) => {
            for x in a {
                strip_cost_fields(x, found);
            }
        }
        _ => {}
    }
}

/// The static rule for which calls become jobs (`crate::jobs`).
fn runs_as_job(tool: &str, args: &Value) -> bool {
    let flag = |k: &str| args.get(k).and_then(Value::as_bool).unwrap_or(false);
    match tool {
        "score.why" | "score.verify" | "score.advise" | "score.record" => !flag("wait"),
        "apply.receipt" | "apply.refresh" => !flag("wait") && !flag("dry_run"),
        _ => false,
    }
}

fn invalid_refusal(tool: &str, e: &serde_json::Error) -> Refusal {
    Refusal::new(
        RefusalCode::InvalidInput,
        format!("the arguments do not match {tool}'s input schema ({e})"),
        json!({ "errors": [e.to_string()] }),
    )
}

fn key_file(path: String, columns: Option<Vec<String>>) -> KeyFile {
    let f = KeyFile::new(path);
    match columns {
        Some(c) => f.with_columns(c),
        None => f,
    }
}

fn resolve_keys(
    path: Option<String>,
    columns: Option<Vec<String>>,
    key_values: Option<Vec<Vec<Value>>>,
) -> bellwether::types::Result<KeyFile> {
    match (path, key_values) {
        (Some(_), Some(_)) => Err(Error::InvalidArgument {
            field: "key_values",
            reason: "give `keys` (a Parquet key file) or `key_values` (inline key tuples), not \
                     both"
                .into(),
        }),
        (None, None) => Err(Error::InvalidArgument {
            field: "keys",
            reason: "give `keys` (a Parquet key file) or `key_values` (inline key tuples with \
                     `key_columns`)"
                .into(),
        }),
        (Some(p), None) => Ok(key_file(p, columns)),
        (None, Some(rows)) => {
            if rows.is_empty() {
                return Err(Error::InvalidArgument {
                    field: "key_values",
                    reason: "`key_values` is empty; pass at least one key tuple".into(),
                });
            }
            let Some(cols) = columns.filter(|c| !c.is_empty()) else {
                return Err(Error::InvalidArgument {
                    field: "key_columns",
                    reason: "`key_values` requires `key_columns` naming the key column(s) in \
                             tuple order"
                        .into(),
                });
            };
            for (r, row) in rows.iter().enumerate() {
                if row.len() != cols.len() {
                    return Err(Error::InvalidArgument {
                        field: "key_values",
                        reason: format!(
                            "`key_values` row {r} has {} value(s), but `key_columns` names {} \
                             column(s) ({})",
                            row.len(),
                            cols.len(),
                            cols.join(", ")
                        ),
                    });
                }
            }
            Ok(KeyFile::from_values(cols, rows))
        }
    }
}

fn with_emit(
    engine: &Engine,
    mut result: bellwether::PlanResult,
    emit: Option<EmitEngine>,
    ordered: bool,
    residual: Option<EmitResidual>,
    relocate: Option<[String; 2]>,
) -> bellwether::types::Result<bellwether::PlanResult> {
    let Some(target) = emit else {
        if ordered {
            return Err(Error::InvalidArgument {
                field: "ordered",
                reason: "`ordered` applies to an emitted artifact; pass `emit` (`datafusion`, \
                         `duckdb`, or `spark`) with it, or omit `ordered`"
                    .into(),
            });
        }
        if residual.is_some() {
            return Err(Error::InvalidArgument {
                field: "residual",
                reason: "`residual` applies to an emitted artifact; pass `emit` (`datafusion`, \
                         `duckdb`, or `spark`) with it, or omit `residual`"
                    .into(),
            });
        }
        if relocate.is_some() {
            return Err(Error::InvalidArgument {
                field: "relocate",
                reason: "`relocate` applies to an emitted artifact; pass `emit` (`datafusion`, \
                         `duckdb`, or `spark`) with it, or omit `relocate`"
                    .into(),
            });
        }
        return Ok(result);
    };
    let emitted = engine.emit_plan_result(
        &result,
        &bellwether::EmitRequest {
            engine: target.into(),
            keys_file: None,
            residual: residual.unwrap_or_default().into(),
            ordered,
            footer_hint: false,
            engine_pruning: false,
            relocate,
        },
    )?;
    result.emitted = Some(emitted);
    Ok(result)
}

fn discover_workspace_datasets(
    ws_root: &std::path::Path,
    datasets: &mut std::collections::BTreeMap<String, String>,
) {
    fn visit(
        ws_root: &std::path::Path,
        dir: &std::path::Path,
        depth: usize,
        datasets: &mut std::collections::BTreeMap<String, String>,
    ) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in rd.flatten() {
            let name_os = entry.file_name();
            let Some(name) = name_os.to_str() else {
                continue;
            };
            if name.starts_with('.') || name == "main" {
                continue;
            }
            let path = entry.path();
            let Ok(canonical) = std::fs::canonicalize(&path) else {
                continue;
            };
            if !canonical.is_dir() || !canonical.starts_with(ws_root) {
                continue;
            }
            let canon_str = canonical.to_string_lossy().into_owned();
            datasets
                .entry(name.to_string())
                .or_insert_with(|| canon_str.clone());
            if let Ok(rel) = path.strip_prefix(ws_root) {
                if let Some(rel_str) = rel.to_str() {
                    datasets.entry(rel_str.to_string()).or_insert(canon_str);
                }
            }
            if depth < 2 {
                visit(ws_root, &canonical, depth + 1, datasets);
            }
        }
    }
    visit(ws_root, ws_root, 0, datasets);
}

fn propose(
    target: Option<Engine>,
    default_engine: &Engine,
    body: ProposalBody,
    evidence: &[bellwether::types::Hash],
    by: &str,
    dry_run: bool,
) -> Done {
    let e = target.as_ref().unwrap_or(default_engine);
    propose_done(
        if dry_run {
            e.propose_dry_run(body, evidence, by)
        } else {
            e.propose(body, evidence, by)
        },
        target,
    )
}

fn propose_done(r: bellwether::types::Result<ProposeResult>, engine: Option<Engine>) -> Done {
    match r {
        Ok(r) => {
            let refusal = match &r {
                ProposeResult::Refused { refusal } => Some(refusal::from_propose(refusal)),
                ProposeResult::Written { .. } | ProposeResult::WouldWrite { .. } => None,
            };
            done(&r, refusal, engine)
        }
        Err(err) => failed(&err),
    }
}

fn fix_done(r: bellwether::types::Result<FixResult>, engine: Option<Engine>) -> Done {
    match r {
        Ok(r) => {
            let refusal = match &r.outcome {
                FixOutcome::Refused { refusal } => {
                    Some(refusal::from_fix(refusal, r.verification.as_ref()))
                }
                FixOutcome::Applied { .. }
                | FixOutcome::Attested { .. }
                | FixOutcome::Verified
                | FixOutcome::WouldApply { .. } => None,
            };
            done(&r, refusal, engine)
        }
        Err(err) => failed(&err),
    }
}
