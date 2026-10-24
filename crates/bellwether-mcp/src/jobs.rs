//! Long operations as jobs (WP12 addition 9). A job id is the BLAKE3 of the
//! request (the tool name and its arguments, keys sorted, `wait` left out),
//! so starting the same request again returns the same job, running or
//! finished, rather than a second run, and an id always names one result:
//! calling the tool again directly with a finished job's request returns
//! that job's stored result. Jobs live for the session; to run a request
//! again now, call it with `wait: true`, which runs in the call and is not
//! a job.
//!
//! Which calls become jobs is a static rule, not a timer, so a result never
//! depends on how fast the machine is: `score.*` and `apply.receipt` run as
//! jobs unless called with `wait: true` (or, for apply, `dry_run: true`);
//! every other tool runs in the call; `jobs.start` runs any tool as a job.
//! A charged job waits for the session's charged call in flight to settle
//! before it starts (`crate::budget`); its progress says so.

use std::collections::BTreeMap;
use std::time::Instant;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::server::Reply;

/// A job's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Still running.
    Running,
    /// Finished; `jobs.result` returns its result.
    Done,
}

/// A job, as the envelope reports it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct JobInfo {
    /// The job id: 64 hex characters, the hash of the request.
    pub id: String,
    /// The tool it runs.
    pub tool: String,
    /// Running or done.
    pub state: JobState,
    /// One line an agent can relay to a human. The facade reports no
    /// progress within an operation, so this names the stage and the
    /// elapsed time.
    pub progress: String,
}

/// One job.
pub(crate) struct Job {
    pub tool: String,
    pub started: Instant,
    /// Waiting for another charged call in this session to settle.
    pub queued: bool,
    pub finished: Option<(Instant, Reply)>,
}

impl Job {
    pub fn info(&self, id: &str) -> JobInfo {
        let (state, progress) = match &self.finished {
            None if self.queued => (
                JobState::Running,
                format!(
                    "{} waiting for {:.1} s: another score or apply call in this session is \
                     running, and the budget admits one at a time (poll jobs.status)",
                    self.tool,
                    self.started.elapsed().as_secs_f64()
                ),
            ),
            None => (
                JobState::Running,
                format!(
                    "{} running for {:.1} s; the engine is working (poll jobs.status)",
                    self.tool,
                    self.started.elapsed().as_secs_f64()
                ),
            ),
            Some((at, reply)) => (
                JobState::Done,
                format!(
                    "{} finished in {:.1} s ({}); read it with jobs.result",
                    self.tool,
                    at.duration_since(self.started).as_secs_f64(),
                    match reply.envelope.status {
                        crate::envelope::Status::Refused => "refused",
                        _ => "ok",
                    }
                ),
            ),
        };
        JobInfo {
            id: id.to_string(),
            tool: self.tool.clone(),
            state,
            progress,
        }
    }
}

/// The jobs of one session.
pub(crate) type Jobs = BTreeMap<String, Job>;

/// The job id of a request: BLAKE3 of `{"arguments": .., "tool": ..}` with
/// every object's keys sorted, so the id does not depend on the order the
/// client sent fields in.
#[must_use]
pub fn job_id(tool: &str, arguments: &Value) -> String {
    let mut s = String::from("{\"arguments\":");
    canonical(arguments, &mut s);
    s.push_str(",\"tool\":");
    canonical(&Value::from(tool), &mut s);
    s.push('}');
    blake3::hash(s.as_bytes()).to_hex().to_string()
}

fn canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            out.push('{');
            for (i, k) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::from(k.as_str()).to_string());
                out.push(':');
                canonical(&m[k], out);
            }
            out.push('}');
        }
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                canonical(x, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}
