//! Per-session budgets (WP12 addition 12): replays written, rows the
//! verifier read, and wall time, so a looping agent cannot burn a machine.
//!
//! A session is one server process (one MCP connection over stdio). Before
//! a tool runs, a spent dimension refuses it (`budget_exceeded`); after it
//! runs, what it consumed is charged from its typed result: Replay records
//! it wrote, the rows in both read sets of any verification it ran, and its
//! elapsed time. The facade's operations are not cancellable, so one call
//! that starts under budget may finish over it; the next is refused.
//!
//! Bytes read are not a dimension: the facade reports no bytes read by an
//! operation (its byte counts are what a lookup would read, not what the
//! engine read), and a guessed number would be a silent wrong answer. Rows
//! verified are exact sums of row-group statistics.

use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The limits, set at startup (`--budget-replays`, `--budget-rows`,
/// `--budget-seconds`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Budget {
    /// Replay records the session may cause the engine to write.
    pub replays: u64,
    /// Rows the verifier may read, summed over both sides.
    pub rows: u64,
    /// Wall time the session's tool calls may take, in seconds.
    pub seconds: u64,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            replays: 256,
            rows: 2_000_000_000,
            seconds: 3_600,
        }
    }
}

/// What a session has spent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Spent {
    /// Replay records written.
    pub replays: u64,
    /// Rows the verifier read.
    pub rows: u64,
    /// Milliseconds of tool calls.
    pub millis: u64,
}

/// The budget and what was spent, as the envelope reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BudgetState {
    /// The limits.
    pub limit: Budget,
    /// Spent so far.
    pub spent: Spent,
}

impl BudgetState {
    /// The first spent dimension, as `(name, spent, limit)`.
    #[must_use]
    pub fn exceeded(&self) -> Option<(&'static str, u64, u64)> {
        let s = &self.spent;
        let l = &self.limit;
        if s.replays >= l.replays {
            return Some(("replays", s.replays, l.replays));
        }
        if s.rows >= l.rows {
            return Some(("rows", s.rows, l.rows));
        }
        let secs = s.millis / 1_000;
        if secs >= l.seconds {
            return Some(("seconds", secs, l.seconds));
        }
        None
    }

    /// Charges one call: its result's JSON (for counting only) and its
    /// elapsed time.
    pub fn charge(&mut self, result: Option<&Value>, elapsed: Duration) {
        if let Some(v) = result {
            let (replays, rows) = consumed(v);
            self.spent.replays = self.spent.replays.saturating_add(replays);
            self.spent.rows = self.spent.rows.saturating_add(rows);
        }
        let ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        self.spent.millis = self.spent.millis.saturating_add(ms);
    }
}

/// Replay records a result embeds (`{hash, record: {record: "replay"}}`)
/// and rows in the read sets of any verification (`sides[].rows`).
fn consumed(v: &Value) -> (u64, u64) {
    let mut replays = 0u64;
    let mut rows = 0u64;
    let mut stack = vec![v];
    while let Some(v) = stack.pop() {
        match v {
            Value::Object(m) => {
                if m.contains_key("hash")
                    && m.get("record").and_then(|r| r.get("record")) == Some(&Value::from("replay"))
                {
                    replays += 1;
                }
                if let Some(Value::Array(sides)) = m.get("sides") {
                    for s in sides {
                        rows = rows.saturating_add(
                            s.get("rows").and_then(Value::as_u64).unwrap_or_default(),
                        );
                    }
                }
                stack.extend(m.values());
            }
            Value::Array(a) => stack.extend(a.iter()),
            _ => {}
        }
    }
    (replays, rows)
}
