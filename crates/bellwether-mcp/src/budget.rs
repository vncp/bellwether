//! Per-session budgets (WP12 addition 12): replays written, rows the
//! verifier read, and wall time, so a looping agent cannot burn a machine.
//!
//! A session is one server process (one MCP connection over stdio). Every
//! call is checked before it runs: a spent dimension refuses it
//! (`budget_exceeded`). After it runs, what it consumed is charged from its
//! typed result: Replay records it wrote, the rows in both read sets of any
//! verification it ran, and its elapsed time.
//!
//! A charged call (a score or apply call that is not a dry run: the calls
//! that write Replays or read rows) is admitted atomically: under the budget
//! lock it is checked and reserves every replay and row the session has
//! left. A second charged call that arrives while the reservation is held
//! waits for it to settle, then is checked against what the first call
//! actually spent; settling charges the call and refunds the unused part of
//! the reservation. So concurrent jobs behave as if run one at a time: the
//! facade's operations are not cancellable, so the call that crosses a limit
//! finishes over it, and it is the only one; the next is refused.
//!
//! Bytes read are not a dimension: the facade reports no bytes read by an
//! operation (its byte counts are what a lookup would read, not what the
//! engine read), and a guessed number would be a silent wrong answer. Rows
//! verified are exact sums of row-group statistics.

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
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

/// What a session has spent, or what a call in flight holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Spent {
    /// Replay records written.
    pub replays: u64,
    /// Rows the verifier read.
    pub rows: u64,
    /// Milliseconds of tool calls.
    pub millis: u64,
}

/// The budget, what was spent, and what the charged call in flight holds,
/// as the envelope reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BudgetState {
    /// The limits.
    pub limit: Budget,
    /// Spent so far.
    pub spent: Spent,
    /// Reserved by the charged call in flight: the replays and rows left
    /// when it was admitted. Zero when none is running.
    pub reserved: Spent,
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

/// The dimension a call was refused on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Exceeded {
    /// `replays`, `rows`, or `seconds`.
    pub dimension: &'static str,
    /// Spent in it.
    pub spent: u64,
    /// Its limit.
    pub limit: u64,
}

/// The session's budget: the state behind a lock, and the signal a waiting
/// charged call is woken by.
#[derive(Debug)]
pub struct Meter {
    state: Mutex<BudgetState>,
    settled: Condvar,
}

fn lock(m: &Mutex<BudgetState>) -> MutexGuard<'_, BudgetState> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Meter {
    /// A fresh session with `limit`.
    #[must_use]
    pub fn new(limit: Budget) -> Self {
        Self {
            state: Mutex::new(BudgetState {
                limit,
                spent: Spent::default(),
                reserved: Spent::default(),
            }),
            settled: Condvar::new(),
        }
    }

    /// The budget now.
    #[must_use]
    pub fn state(&self) -> BudgetState {
        *lock(&self.state)
    }

    /// Admits a call, or refuses it with the spent dimension. A `charged`
    /// call first waits for any reservation in flight to settle (calling
    /// `on_wait` once, without the lock held, if it has to), then is checked
    /// and reserves the replays and rows left, all under one lock.
    pub fn admit(
        &self,
        charged: bool,
        on_wait: impl FnOnce(),
    ) -> Result<Reservation<'_>, Exceeded> {
        let mut on_wait = Some(on_wait);
        let mut s = lock(&self.state);
        while charged && s.reserved != Spent::default() {
            if let Some(f) = on_wait.take() {
                drop(s);
                f();
                s = lock(&self.state);
                continue;
            }
            s = self.settled.wait(s).unwrap_or_else(PoisonError::into_inner);
        }
        if let Some((dimension, spent, limit)) = s.exceeded() {
            return Err(Exceeded {
                dimension,
                spent,
                limit,
            });
        }
        if charged {
            s.reserved = Spent {
                replays: s.limit.replays - s.spent.replays,
                rows: s.limit.rows - s.spent.rows,
                millis: 0,
            };
        }
        Ok(Reservation {
            meter: self,
            charged,
            settled: false,
        })
    }
}

/// An admitted call. Settling it charges what the call consumed and
/// releases its reservation (refunding what it did not use); dropping it
/// unsettled releases the reservation and charges nothing.
#[derive(Debug)]
pub struct Reservation<'a> {
    meter: &'a Meter,
    charged: bool,
    settled: bool,
}

impl Reservation<'_> {
    /// Charges the call's result (`None` when it failed) and elapsed time.
    pub fn settle(mut self, result: Option<&Value>, elapsed: Duration) {
        self.release(result, elapsed);
    }

    fn release(&mut self, result: Option<&Value>, elapsed: Duration) {
        if self.settled {
            return;
        }
        self.settled = true;
        let mut s = lock(&self.meter.state);
        s.charge(result, elapsed);
        if self.charged {
            s.reserved = Spent::default();
            drop(s);
            self.meter.settled.notify_all();
        }
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.release(None, Duration::ZERO);
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn replays(n: usize) -> Value {
        let one = serde_json::json!({"hash": "h", "record": {"record": "replay"}});
        Value::Array(vec![one; n])
    }

    #[test]
    fn each_dimension_refuses_once_spent() {
        for (limit, result, millis, dimension) in [
            (
                Budget {
                    replays: 2,
                    ..Budget::default()
                },
                replays(2),
                0,
                "replays",
            ),
            (
                Budget {
                    rows: 20,
                    ..Budget::default()
                },
                serde_json::json!({"sides": [{"rows": 12}, {"rows": 8}]}),
                0,
                "rows",
            ),
            (
                Budget {
                    seconds: 1,
                    ..Budget::default()
                },
                Value::Null,
                1_000,
                "seconds",
            ),
        ] {
            let m = Meter::new(limit);
            m.admit(true, || {})
                .unwrap()
                .settle(Some(&result), Duration::from_millis(millis));
            let e = m.admit(false, || {}).unwrap_err();
            assert_eq!(e.dimension, dimension);
            assert_eq!(m.state().reserved, Spent::default());
        }
        // One under every limit is admitted.
        let m = Meter::new(Budget {
            replays: 3,
            rows: 21,
            seconds: 2,
        });
        m.admit(true, || {}).unwrap().settle(
            Some(&serde_json::json!({"a": replays(2), "sides": [{"rows": 20}]})),
            Duration::from_millis(1_999),
        );
        assert_eq!(
            m.state().spent,
            Spent {
                replays: 2,
                rows: 20,
                millis: 1_999
            }
        );
        assert!(m.admit(false, || {}).is_ok());
    }

    #[test]
    fn a_charged_call_reserves_what_is_left_and_refunds_the_rest() {
        let m = Meter::new(Budget {
            replays: 5,
            rows: 100,
            seconds: 60,
        });
        let r = m.admit(true, || {}).unwrap();
        assert_eq!(
            m.state().reserved,
            Spent {
                replays: 5,
                rows: 100,
                millis: 0
            }
        );
        // An uncharged call is not held up by the reservation.
        m.admit(false, || panic!("an uncharged call waited"))
            .unwrap()
            .settle(None, Duration::ZERO);
        r.settle(Some(&replays(1)), Duration::ZERO);
        let s = m.state();
        assert_eq!(s.reserved, Spent::default());
        assert_eq!(s.spent.replays, 1);
        // Dropped unsettled: the reservation is released, nothing charged.
        drop(m.admit(true, || {}).unwrap());
        assert_eq!(m.state().reserved, Spent::default());
        assert_eq!(m.state().spent.replays, 1);
    }

    /// A charged call that arrives while another holds the reservation
    /// waits for it (reporting `Waited` before anything else), and is then
    /// checked against what the first call spent: one replay on a
    /// one-replay meter refuses it on `replays`. Deterministic: the held
    /// reservation settles only after the waiter has reported waiting.
    #[test]
    fn a_charged_call_waits_for_the_held_reservation_then_is_refused() {
        use std::sync::mpsc;

        #[derive(Debug, PartialEq)]
        enum Event {
            Waited,
            Done(Result<(), &'static str>),
        }
        let m = Meter::new(Budget {
            replays: 1,
            ..Budget::default()
        });
        let held = m.admit(true, || panic!("the first call waited")).unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let meter = &m;
            scope.spawn(move || {
                let waited = tx.clone();
                let r = meter.admit(true, move || waited.send(Event::Waited).unwrap());
                tx.send(Event::Done(r.map(|_| ()).map_err(|e| e.dimension)))
                    .unwrap();
            });
            // Nothing settles until the waiter has said it waits.
            assert_eq!(rx.recv().unwrap(), Event::Waited);
            assert!(
                rx.recv_timeout(Duration::from_millis(50)).is_err(),
                "admitted while the reservation was held"
            );
            held.settle(Some(&replays(1)), Duration::ZERO);
            assert_eq!(rx.recv().unwrap(), Event::Done(Err("replays")));
        });
        let s = m.state();
        assert_eq!(s.spent.replays, 1);
        assert_eq!(s.reserved, Spent::default());
    }
}
