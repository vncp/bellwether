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
    Engine, EngineOptions, FixOutcome, FixRequest, FixResult, KeyFile, ProposeResult, WhyRequest,
    DEFAULT_MAX_CANDIDATES,
};
use serde::Serialize;
use serde_json::{json, Value};

use crate::budget::{Budget, BudgetState, Exceeded, Meter};
use crate::envelope::{cites_of, Chain, Envelope, Status};
use crate::jobs::{job_id, Job, JobInfo, JobState, Jobs};
use crate::refusal::{self, Refusal, RefusalCode};
use crate::tools::{
    self, ApplyReceipt, JobRef, JobsStart, ObserveAsof, ObserveLocate, ObserveWhy, ProposeKey,
    ProposeLayout, ProposeRewrite, ScoreVerify, ScoreWhy, Tier,
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
    },
    /// Refused before or by the facade with an error.
    Failed(Refusal),
}

fn done<T: Serialize>(r: &T, refusal: Option<Refusal>) -> Done {
    match (serde_json::to_string(r), serde_json::to_value(r)) {
        (Ok(text), Ok(value)) => Done::Result {
            text,
            value,
            refusal,
        },
        (Err(e), _) | (_, Err(e)) => Done::Failed(refusal::from_error(&Error::Serde(e))),
    }
}

fn failed(e: &Error) -> Done {
    Done::Failed(refusal::from_error(e))
}

struct Inner {
    engine: Engine,
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

impl Server {
    /// Opens the engine over `config.root`. Opening writes nothing.
    pub fn new(config: &Config) -> bellwether::types::Result<Self> {
        let engine = Engine::open(&config.root, config.engine.clone())?;
        Ok(Self {
            inner: Arc::new(Inner {
                engine,
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
                Refusal::new(
                    RefusalCode::InvalidInput,
                    format!("the arguments do not match {tool}'s input schema"),
                    json!({ "errors": errors }),
                ),
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
            } => {
                reservation.settle(Some(&value), elapsed);
                let (cites, receipts) = cites_of(&value);
                let chains = receipts
                    .iter()
                    .filter_map(|h| self.inner.engine.receipt(h).ok())
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
        let e = &self.inner.engine;
        macro_rules! parse {
            ($t:ty) => {
                match serde_json::from_value::<$t>(args) {
                    Ok(a) => a,
                    Err(err) => return Done::Failed(invalid_refusal(tool, &err)),
                }
            };
        }
        match tool {
            "observe.why" => {
                let a = parse!(ObserveWhy);
                match e.why(&WhyRequest {
                    columns: a.columns,
                    ..WhyRequest::default()
                }) {
                    Ok(r) => done(&r, None),
                    Err(err) => failed(&err),
                }
            }
            "observe.profile" => match e.findings() {
                Ok(r) => done(&r, None),
                Err(err) => failed(&err),
            },
            "observe.locate" => {
                let a = parse!(ObserveLocate);
                match e.locate(&key_file(a.keys, a.key_columns)) {
                    Ok(r) => done(&r, None),
                    Err(err) => failed(&err),
                }
            }
            "observe.asof" => {
                let a = parse!(ObserveAsof);
                match e.asof(&key_file(a.keys, a.key_columns), &a.at) {
                    Ok(r) => done(&r, None),
                    Err(err) => failed(&err),
                }
            }
            "score.why" => {
                let a = parse!(ScoreWhy);
                if a.keys.is_none() && a.workload.is_none() {
                    return Done::Failed(Refusal::new(
                        RefusalCode::NoWorkload,
                        "score.why replays a workload; without one, call observe.why",
                        json!({}),
                    ));
                }
                let request = WhyRequest {
                    keys: a.keys.map(|k| key_file(k, a.key_columns)),
                    bench_workload: None,
                    workload: a.workload,
                    columns: a.columns,
                    max_candidates: a
                        .max_candidates
                        .map_or(DEFAULT_MAX_CANDIDATES, |n| n as usize),
                };
                match e.why(&request) {
                    Ok(r) => done(&r, None),
                    Err(err) => failed(&err),
                }
            }
            "score.verify" => {
                let a = parse!(ScoreVerify);
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
                };
                fix_done(e.fix(&request))
            }
            "propose.key" => {
                let a = parse!(ProposeKey);
                propose(
                    e,
                    ProposalBody::Key { spec: a.spec },
                    &a.evidence,
                    &a.by,
                    a.dry_run,
                )
            }
            "propose.layout" => {
                let a = parse!(ProposeLayout);
                propose(
                    e,
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
                propose(
                    e,
                    ProposalBody::Rewrite { shape: a.shape },
                    &a.evidence,
                    &a.by,
                    a.dry_run,
                )
            }
            "apply.receipt" => {
                let a = parse!(ApplyReceipt);
                if a.dry_run {
                    fix_done(e.apply_dry_run(&a.certificate))
                } else {
                    fix_done(e.fix(&FixRequest {
                        certificate: Some(a.certificate),
                        ..FixRequest::default()
                    }))
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
                Refusal::new(
                    RefusalCode::InvalidInput,
                    format!("the arguments do not match {tool}'s input schema"),
                    json!({ "errors": errors }),
                ),
            );
        }
        let mut key = args.clone();
        if let Value::Object(m) = &mut key {
            // `wait` changes how the result is delivered, not what runs.
            m.remove("wait");
        }
        let id = job_id(tool, &key);
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

/// The static rule for which calls become jobs (`crate::jobs`).
fn runs_as_job(tool: &str, args: &Value) -> bool {
    let flag = |k: &str| args.get(k).and_then(Value::as_bool).unwrap_or(false);
    match tool {
        "score.why" | "score.verify" => !flag("wait"),
        "apply.receipt" => !flag("wait") && !flag("dry_run"),
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

fn propose(
    e: &Engine,
    body: ProposalBody,
    evidence: &[bellwether::types::Hash],
    by: &str,
    dry_run: bool,
) -> Done {
    let r = if dry_run {
        e.propose_dry_run(body, evidence, by)
    } else {
        e.propose(body, evidence, by)
    };
    match r {
        Ok(r) => {
            let refusal = match &r {
                ProposeResult::Refused { refusal } => Some(refusal::from_propose(refusal)),
                ProposeResult::Written { .. } | ProposeResult::WouldWrite { .. } => None,
            };
            done(&r, refusal)
        }
        Err(err) => failed(&err),
    }
}

fn fix_done(r: bellwether::types::Result<FixResult>) -> Done {
    match r {
        Ok(r) => {
            let refusal = match &r.outcome {
                FixOutcome::Refused { refusal } => {
                    Some(refusal::from_fix(refusal, r.verification.as_ref()))
                }
                FixOutcome::Applied { .. }
                | FixOutcome::Verified
                | FixOutcome::WouldApply { .. } => None,
            };
            done(&r, refusal)
        }
        Err(err) => failed(&err),
    }
}
