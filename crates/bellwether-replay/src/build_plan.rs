//! The build crate's [`Planner`] as a [`PlanSource`]: the planner the index
//! itself answers queries with, so replaying a committed index plans every
//! run exactly as a query would (ADR 011), and a hypothetical spec's
//! transient segments plan through the same code
//! (`Planner::plan_with_transient`).
//!
//! - [`PlanSource::specs`] is the declared specs complete at the pinned
//!   sequence (`Planner::completeness`, which recomputes coverage from the
//!   live segments, ADR 012). An incomplete declared spec is not one the
//!   index can plan with.
//! - [`PlanSource::plan_query`] is `Planner::plan` over the run's lookup:
//!   `Lookup::Keys`, `Latest`, `AsOf`, or `Lookback` of the run's canonical
//!   tuples, or a `Lookup::Window` for a window run and an `asof` lookback
//!   run ([`crate::asof_lookback_window`]). The
//!   planner refuses a listed spec that is not declared exactly or not
//!   complete, and nothing else: an incomplete spec the plan does not use
//!   is no error.
//! - [`PlanSource::plan_text`] is `Planner::plan` over a `Lookup::Text`.
//! - [`PlanSource::with_transient`] passes each transient index's segment
//!   bytes as a `TransientSpec`, after checking that the index was built at
//!   the pinned sequence, that every segment records the dataset's key
//!   types, and that the row-group ranges its segments record cover every
//!   live row group of the pinned manifest (a transient segment records the
//!   row groups it processed, keys or not).

use std::sync::Arc;

use bellwether_build::{Lookup, Planner, TransientSpec};
use bellwether_store::StoreExt;
use bellwether_types::{Error, KeySpec, Result, ScanPlan, TextOp};
use roaring::RoaringBitmap;

use crate::keys::RunKeys;
use crate::plan::{check_key_types, planned_query, text_run, PlanSource};
use crate::transient::TransientIndex;
use crate::workload::RunQuery;

/// The run's keys and query kind as the build planner's lookup: canonical
/// tuples over the run's columns, at the dataset types. `query` is as
/// planned ([`planned_query`]): an `asof` lookback run is already its
/// window.
fn lookup(keys: &RunKeys, query: &RunQuery) -> Result<Lookup> {
    let columns = keys.columns().to_vec();
    let tuples: Vec<Vec<u8>> = keys.tuples().map(<[u8]>::to_vec).collect();
    Ok(match query {
        RunQuery::Locate | RunQuery::Count => Lookup::Keys {
            columns,
            keys: tuples,
        },
        RunQuery::Latest => Lookup::Latest {
            columns,
            keys: tuples,
            live: None,
        },
        RunQuery::Asof { ts } => Lookup::AsOf {
            columns,
            keys: tuples,
            ts: *ts,
            live: None,
        },
        RunQuery::Live(l) => match l.asof {
            None => Lookup::Latest {
                columns,
                keys: tuples,
                live: Some(l.tombstone.clone()),
            },
            Some(ts) => Lookup::AsOf {
                columns,
                keys: tuples,
                ts,
                live: Some(l.tombstone.clone()),
            },
        },
        RunQuery::Lookback { lookback, .. } => Lookup::Lookback {
            columns,
            keys: tuples,
            lookback: *lookback,
        },
        RunQuery::Window(w) => Lookup::Window {
            columns,
            keys: tuples,
            ts_column: w.ts_column.clone(),
            lo: w.lo,
            hi: w.hi,
        },
        // A recorded membership run has exactly one column
        // (`RecordedRun::query`); any other count names no schema column.
        RunQuery::ContainsAny => Lookup::ContainsAny {
            column: columns.join(","),
            values: tuples,
        },
        RunQuery::ContainsAll => Lookup::ContainsAll {
            column: columns.join(","),
            values: tuples,
        },
        RunQuery::Bitemporal(b) => Lookup::Bitemporal {
            columns,
            clocks: Some((b.valid_column.clone(), b.known_column.clone())),
            keys: tuples,
            valid: b.valid,
            known: b.known,
            live: b.tombstone.clone(),
        },
        RunQuery::SemiJoin(_) => {
            return Err(Error::Other(
                "a semijoin run spans two datasets and is planned per side by the replayer, \
                 never as one dataset's lookup"
                    .into(),
            ))
        }
        RunQuery::Text(_) => {
            let (column, op, values) = text_run(keys, query)?
                .ok_or_else(|| Error::Other("a text run without a text match".into()))?;
            text_lookup(column, op, &values)
        }
    })
}

/// A text lookup as the build planner's lookup.
fn text_lookup(column: &str, op: TextOp, values: &[String]) -> Lookup {
    Lookup::Text {
        column: column.to_string(),
        op,
        values: values.to_vec(),
    }
}

/// Checks `transient` against `planner`'s pinned manifest (module docs) and
/// returns each index's segments as a `TransientSpec`.
fn checked<S>(planner: &Planner<S>, transient: &[Arc<TransientIndex>]) -> Result<Vec<TransientSpec>>
where
    S: StoreExt + ?Sized,
{
    let sequence = planner.sequence().to_string();
    let live: RoaringBitmap = planner
        .manifest()
        .rowgroup_space
        .files
        .iter()
        .flat_map(|f| f.rowgroup_ids.iter().map(|id| id.0))
        .collect();
    let mut out = Vec::with_capacity(transient.len());
    for t in transient {
        let name = t.spec().name();
        if t.manifest_version() != sequence {
            return Err(Error::Other(format!(
                "transient index for {name} was built at manifest {}, not the pinned {sequence}",
                t.manifest_version()
            )));
        }
        let mut covered = RoaringBitmap::new();
        for r in t.segments().readers() {
            r.check_dataset(planner.schema())?;
            if let Some((first, last)) = r.rowgroup_range() {
                covered.insert_range(first.0..=last.0);
            }
        }
        let gap = &live - &covered;
        if let Some(first) = gap.min() {
            return Err(Error::KeySpec(format!(
                "transient index for {name} covers no segment of live row group {first} ({} \
                 uncovered) of manifest {sequence}; it was built over another row-group set",
                gap.len()
            )));
        }
        out.push(TransientSpec {
            spec: t.spec().clone(),
            segments: t.segment_bytes(),
            bounds: t.bounds_bytes().to_vec(),
            tombstones: t.tombstone_bytes().to_vec(),
        });
    }
    Ok(out)
}

impl<S> PlanSource for Planner<S>
where
    S: StoreExt + ?Sized + Send + Sync,
{
    fn manifest_version(&self) -> String {
        self.sequence().to_string()
    }

    /// The specs complete at the pinned sequence, as `Planner::usable_specs`
    /// gives them: a spec with a tombstone (B8) whose tombstone sidecar is
    /// not complete plans without it, as the facade's lookups do.
    fn specs(&self) -> Vec<KeySpec> {
        self.usable_specs().0
    }

    fn plan_query(&self, specs: &[KeySpec], keys: &RunKeys, query: &RunQuery) -> Result<ScanPlan> {
        check_key_types(self.schema(), keys)?;
        let query = planned_query(self.schema(), specs, keys.columns(), query)?;
        Planner::plan(self, specs, &lookup(keys, &query)?)
    }

    fn plan_text(
        &self,
        specs: &[KeySpec],
        column: &str,
        op: TextOp,
        values: &[String],
    ) -> Result<ScanPlan> {
        Planner::plan(self, specs, &text_lookup(column, op, values))
    }

    fn lookback_column(&self, specs: &[KeySpec], run_columns: &[String]) -> Result<Option<String>> {
        Planner::lookback_column(self, specs, run_columns)
    }

    fn window_column(&self, specs: &[KeySpec], run_columns: &[String]) -> Result<Option<String>> {
        Planner::window_column(self, specs, run_columns)
    }

    fn with_transient(
        &self,
        transient: &[Arc<TransientIndex>],
    ) -> Result<Box<dyn PlanSource + '_>> {
        let extra = checked(self, transient)?;
        Ok(Box::new(WithTransient {
            planner: self,
            extra,
        }))
    }
}

/// The build planner with transient specs added.
struct WithTransient<'a, S: StoreExt + ?Sized> {
    planner: &'a Planner<S>,
    extra: Vec<TransientSpec>,
}

impl<S> PlanSource for WithTransient<'_, S>
where
    S: StoreExt + ?Sized + Send + Sync,
{
    fn manifest_version(&self) -> String {
        self.planner.sequence().to_string()
    }

    fn specs(&self) -> Vec<KeySpec> {
        let mut out = PlanSource::specs(self.planner);
        for t in &self.extra {
            if !out.contains(&t.spec) {
                out.push(t.spec.clone());
            }
        }
        out
    }

    fn plan_query(&self, specs: &[KeySpec], keys: &RunKeys, query: &RunQuery) -> Result<ScanPlan> {
        check_key_types(self.planner.schema(), keys)?;
        let query = planned_query(self.planner.schema(), specs, keys.columns(), query)?;
        self.planner
            .plan_with_transient(specs, &lookup(keys, &query)?, &self.extra)
    }

    fn plan_text(
        &self,
        specs: &[KeySpec],
        column: &str,
        op: TextOp,
        values: &[String],
    ) -> Result<ScanPlan> {
        self.planner
            .plan_with_transient(specs, &text_lookup(column, op, values), &self.extra)
    }

    fn lookback_column(&self, specs: &[KeySpec], run_columns: &[String]) -> Result<Option<String>> {
        self.planner.lookback_column(specs, run_columns)
    }

    fn window_column(&self, specs: &[KeySpec], run_columns: &[String]) -> Result<Option<String>> {
        self.planner.window_column(specs, run_columns)
    }

    fn with_transient(
        &self,
        transient: &[Arc<TransientIndex>],
    ) -> Result<Box<dyn PlanSource + '_>> {
        let mut extra = self.extra.clone();
        for t in checked(self.planner, transient)? {
            if extra.iter().any(|e| e.spec == t.spec) {
                return Err(Error::KeySpec(format!(
                    "key spec {:?} already has transient segments in this planner",
                    t.spec
                )));
            }
            extra.push(t);
        }
        Ok(Box::new(WithTransient {
            planner: self.planner,
            extra,
        }))
    }
}
