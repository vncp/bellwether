//! The plan-building seam ([`PlanSource`]) and the in-crate planner over
//! core segments ([`SegmentPlanner`]).
//!
//! Replay never estimates (ADR 005): a run's cost is the exact sum of the
//! statistics of the row groups a plan selects. The plan comes from a
//! [`PlanSource`]. The build crate's planner implements it
//! ([`crate::build_plan`]); [`SegmentPlanner`] is this crate's
//! implementation over core segments, and follows the build planner's
//! semantics, so both produce the plans the index produces
//! (`tests/wp4_planner.rs`):
//!
//! - **Serving** ([`servable`]). A key spec over columns `P` serves a run
//!   keyed on columns `C` when (a) every column of `P` is in `C`: each key
//!   tuple is projected onto `P`, in `P`'s order, and probed (exact,
//!   temporal, bloom, and range, whose `P` is one column); or (b) `C` is a
//!   strict prefix of `P` and the spec is exact or temporal: each key tuple
//!   is a prefix lookup (ADR 002). Nothing else serves.
//! - **Probes.** A float-zero element expands to both the `+0.0` and the
//!   `-0.0` encoding (`docs/format.md`, statistics semantics).
//! - **Intersection** (ADR 001). With several serving specs, each key
//!   tuple's row groups are the intersection of what every serving spec
//!   returns for it, and the plan is the union over tuples.
//! - **No pruning.** An empty spec list plans every live row group (the
//!   raw-scan configuration). A non-empty list of which no spec serves the
//!   run is an error; the caller decides what an unserved run costs.
//! - **Completeness** (ADR 012). A committed spec plans only if it is
//!   complete at the pinned sequence and its segments' row-group ranges cover
//!   every live row group. A plan that lists any other committed spec is an
//!   error, never a partial plan; a plan that does not list it is
//!   unaffected.
//! - **Ids.** Tombstoned ids in a bitmap are dropped; an id that is neither
//!   live nor tombstoned is an error ([`Catalog::plan_for`]).
//! - **Types.** Keys recorded under another type than the dataset declares
//!   are refused, and so is a transient index whose segments record other
//!   key or timestamp types: a probe at another decimal scale or time unit
//!   would silently miss.
//! - **Residual** (ADR 011). Every plan carries `In` over the run's key
//!   tuples (float zeros in both encodings); an empty key set plans zero row
//!   groups with an empty `In`.
//! - **Windows.** A `window` run ([`RunWindow`]) also prunes by its time
//!   column, as the build planner's `Lookup::Window` does: specs serving
//!   that column ([`serves_window`]: a range spec on it, or an exact or
//!   temporal spec led by it), the version chains of a temporal spec on
//!   the run's key columns and that time column, and the values the
//!   folder's partitioning gives the column from file paths
//!   ([`SegmentPlanner::with_partitioning`]); each set is intersected per
//!   key tuple. Its residual is `And[In, Between]` on the time column. An
//!   `asof` lookback run is planned as the window `[asof − lookback, asof]`
//!   on the column [`planned_time_column`] gives (the timestamp of the
//!   temporal spec serving its keys, else the planner's
//!   [`PlanSource::window_column`]); a `latest` lookback run is per key,
//!   from that spec's version chains (or, through the build planner,
//!   per-key bounds: [`PlanSource::lookback_column`]).

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use bellwether_build::{encode_ts, AggLookup};
use bellwether_core::encoding;
use bellwether_core::segment::{ColumnType, ReadAt, SegmentReader};
use bellwether_core::temporal::{self, LookbackHit, VersionChain, VersionHit};
use bellwether_core::text;
use bellwether_core::tombstone;
use bellwether_source::Partitioning;
use bellwether_store::{Snapshot, StoreExt};
use bellwether_types::{
    Error, Hash, KeySpec, Predicate, Quantity, Result, RunCost, ScanPlan, TextOp,
};
use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::catalog::Catalog;
use crate::keys::RunKeys;
use crate::transient::TransientIndex;
use crate::workload::{RunAggregate, RunBitemporal, RunQuery, RunWindow};

/// How a key spec serves a run's keys ([`servable`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// Probe each key tuple projected onto the spec's columns: indexes into
    /// the run's key columns, in the spec's column order.
    Point {
        /// Run key-column index for each spec column.
        project: Vec<usize>,
    },
    /// The run's columns are a strict prefix of the spec's: each whole key
    /// tuple is a prefix lookup (exact and temporal specs only).
    Prefix,
}

/// How `spec` serves a run keyed on `run_columns`, or `None` if it cannot
/// (module docs, rule (a) then rule (b)). An `expr` key serves exactly the
/// runs keyed on its (normalized) expression, never one naming its virtual
/// column. A list key serves only membership runs ([`serves_query`]),
/// never a lookup on the list column's value as a whole.
#[must_use]
pub fn servable(spec: &KeySpec, run_columns: &[String]) -> Option<Lookup> {
    // A text key serves text lookups only (`PlanSource::plan_text`), never
    // a key: its dictionary holds tokens, not values. A list key serves
    // membership runs only (`serves_query`). A join key (B3) serves
    // semi-join runs only (`serves_query`). An agg key serves aggregate
    // runs only (`PlanSource::aggregate`).
    if run_columns.is_empty()
        || matches!(
            spec,
            KeySpec::Text { .. }
                | KeySpec::List { .. }
                | KeySpec::Join { .. }
                | KeySpec::Agg { .. }
        )
    {
        return None;
    }
    if let KeySpec::Expr { expr, .. } = spec {
        return (run_columns == std::slice::from_ref(expr))
            .then(|| Lookup::Point { project: vec![0] });
    }
    let p = spec.columns();
    let project: Option<Vec<usize>> = p
        .iter()
        .map(|c| run_columns.iter().position(|r| r == c))
        .collect();
    if let Some(project) = project {
        return Some(Lookup::Point { project });
    }
    let prefix = matches!(spec, KeySpec::Exact { .. } | KeySpec::Temporal { .. })
        && run_columns.len() < p.len()
        && p[..run_columns.len()] == *run_columns;
    prefix.then_some(Lookup::Prefix)
}

/// Whether `spec` bounds a window on `ts_column` by itself: a range spec
/// on it, or an exact or temporal spec led by it (a prefix range).
#[must_use]
pub fn serves_window(spec: &KeySpec, ts_column: &str) -> bool {
    match spec {
        KeySpec::Range { column } => column == ts_column,
        KeySpec::Exact { columns, .. } | KeySpec::Temporal { columns, .. } => {
            columns.first().is_some_and(|c| c == ts_column)
        }
        KeySpec::Bloom { .. }
        | KeySpec::Bitemporal { .. }
        | KeySpec::Expr { .. }
        | KeySpec::Text { .. }
        | KeySpec::List { .. }
        | KeySpec::Join { .. }
        | KeySpec::Agg { .. } => false,
    }
}

/// Whether `spec` serves a run of query kind `query` keyed on `run_columns`.
///
/// Point (`locate`) runs use [`servable`] (projection or prefix); a
/// `window` run also takes the specs serving its time column
/// ([`serves_window`]). `latest`, `asof`, and lookback runs need a temporal
/// spec whose columns are all in `run_columns` (`Lookup::Point`); other
/// specs that serve `run_columns` also participate by per-key intersection
/// when paired with one temporal spec. A membership run (`contains_any`,
/// `contains_all`) is served by a list key on its one column, and by
/// nothing else. An aggregate run is served by an agg key that answers it
/// ([`aggregate_specs`]) and, read as a `locate`, by what serves its keys.
#[must_use]
pub fn serves_query(spec: &KeySpec, run_columns: &[String], query: &RunQuery) -> bool {
    match query {
        RunQuery::Text(op) => match run_columns {
            [c] => text::serves(spec, c, *op),
            _ => false,
        },
        RunQuery::Locate | RunQuery::Count => servable(spec, run_columns).is_some(),
        RunQuery::Window(w) => {
            servable(spec, run_columns).is_some() || serves_window(spec, &w.ts_column)
        }
        RunQuery::Latest
        | RunQuery::Asof { .. }
        | RunQuery::Lookback { .. }
        | RunQuery::Live(_) => match spec {
            KeySpec::Temporal { .. } => {
                matches!(servable(spec, run_columns), Some(Lookup::Point { .. }))
            }
            _ => servable(spec, run_columns).is_some(),
        },
        RunQuery::ContainsAny | RunQuery::ContainsAll => {
            matches!(spec, KeySpec::List { column } if run_columns == std::slice::from_ref(column))
        }
        RunQuery::Bitemporal(b) => serves_bitemporal(spec, run_columns, b),
        // A semi-join run (B3) is served by the join key on its pair, and
        // each side, without one, by the keys serving a locate there.
        RunQuery::SemiJoin(j) => matches!(
            spec,
            KeySpec::Join { columns, right, right_columns, .. }
                if columns.as_slice() == run_columns
                    && *right == j.right
                    && *right_columns == j.right_columns
        ),
        RunQuery::Aggregate(a) => {
            servable(spec, run_columns).is_some()
                || bellwether_build::serving_agg(
                    std::slice::from_ref(spec),
                    &agg_lookup_of(run_columns, Vec::new(), a),
                )
                .is_some()
        }
    }
}

/// The build planner's aggregate lookup of an aggregate run's keys.
fn agg_lookup_of(columns: &[String], keys: Vec<Vec<u8>>, a: &RunAggregate) -> AggLookup {
    AggLookup {
        columns: columns.to_vec(),
        keys,
        value: a.value.clone(),
        functions: a.functions.clone(),
    }
}

/// The agg key and the counted key among `specs` that answer an aggregate
/// run on `run_columns` from the index, by the build planner's rules
/// (`bellwether_build::serving_agg`, `counting_spec`): an agg key on the
/// run's value storing every function asked, its key columns among the
/// run's, beside a spec counting rows on exactly the run's columns. `None`
/// when either is missing: the run is then read as a `locate`.
#[must_use]
pub fn aggregate_specs(
    specs: &[KeySpec],
    run_columns: &[String],
    a: &RunAggregate,
) -> Option<(KeySpec, KeySpec)> {
    let agg = bellwether_build::serving_agg(specs, &agg_lookup_of(run_columns, Vec::new(), a))?;
    let counting = bellwether_build::counting_spec(specs, run_columns)?;
    Some((agg.clone(), counting.clone()))
}

/// Whether `spec` is a bitemporal key on `run`'s two clocks whose key
/// columns the run names: the only spec that plans a bitemporal run.
#[must_use]
pub fn serves_bitemporal(spec: &KeySpec, run_columns: &[String], run: &RunBitemporal) -> bool {
    matches!(spec, KeySpec::Bitemporal { columns, valid_column, known_column, .. }
        if *valid_column == run.valid_column
            && *known_column == run.known_column
            && columns.iter().all(|c| run_columns.contains(c)))
}

/// The timestamp column of the one temporal spec of `specs` whose columns
/// are all in `run_columns`; `None` when there is none, and more than one
/// is an error, as for `latest` and `asof`.
pub fn serving_temporal_column(
    specs: &[KeySpec],
    run_columns: &[String],
) -> Result<Option<String>> {
    let mut temporal = specs.iter().filter_map(|s| match s {
        KeySpec::Temporal { ts_column, .. }
            if matches!(servable(s, run_columns), Some(Lookup::Point { .. })) =>
        {
            Some(ts_column)
        }
        _ => None,
    });
    match (temporal.next(), temporal.next()) {
        (None, _) => Ok(None),
        (Some(ts), None) => Ok(Some(ts.clone())),
        (Some(_), Some(_)) => Err(Error::KeySpec(
            "more than one temporal key spec serves this lookup; name one".into(),
        )),
    }
}

/// The time column `query` is planned on for a run keyed on `run_columns`
/// under `specs` through `planner`, by the rules the lookup verbs use:
/// `latest` and `asof` take the serving temporal spec's
/// ([`serving_temporal_column`]); a `latest` lookback the planner's
/// [`PlanSource::lookback_column`]; an `asof` lookback the serving temporal
/// spec's, else the planner's [`PlanSource::window_column`]. `None` for
/// `locate` and window runs, and when nothing can plan the query (the run
/// then reads every row group).
pub fn planned_time_column(
    planner: &dyn PlanSource,
    specs: &[KeySpec],
    run_columns: &[String],
    query: &RunQuery,
) -> Result<Option<String>> {
    if let Some(lookback) = query.lookback() {
        check_lookback(lookback)?;
    }
    match query {
        RunQuery::Locate
        | RunQuery::Window(_)
        | RunQuery::Count
        | RunQuery::Text(_)
        | RunQuery::ContainsAny
        | RunQuery::ContainsAll
        | RunQuery::SemiJoin(_)
        | RunQuery::Aggregate(_) => Ok(None),
        RunQuery::Latest | RunQuery::Asof { .. } | RunQuery::Live(_) => {
            serving_temporal_column(specs, run_columns)
        }
        // Planned on its valid clock when a bitemporal key on its clocks
        // serves it; unplannable (every row group) otherwise.
        RunQuery::Bitemporal(b) => Ok(specs
            .iter()
            .any(|s| serves_bitemporal(s, run_columns, b))
            .then(|| b.valid_column.clone())),
        RunQuery::Lookback { asof: None, .. } => planner.lookback_column(specs, run_columns),
        RunQuery::Lookback { asof: Some(_), .. } => {
            match serving_temporal_column(specs, run_columns)? {
                Some(ts) => Ok(Some(ts)),
                None => planner.window_column(specs, run_columns),
            }
        }
    }
}

/// The fixed window an `asof` lookback run is: `[max(at − lookback, floor),
/// at]` on the timestamp column of the one temporal spec of `specs` whose
/// columns are all in `run_columns` ([`serving_temporal_column`]), `floor`
/// being the smallest value that column's type holds. `None` when no
/// temporal spec serves the run.
pub fn asof_lookback_window(
    schema: &Schema,
    specs: &[KeySpec],
    run_columns: &[String],
    at: i64,
    lookback: i64,
) -> Result<Option<RunWindow>> {
    check_lookback(lookback)?;
    serving_temporal_column(specs, run_columns)?
        .map(|ts_column| lookback_window(schema, ts_column, at, lookback))
        .transpose()
}

/// A lookback window's length is not negative.
fn check_lookback(lookback: i64) -> Result<()> {
    if lookback < 0 {
        return Err(Error::KeySpec(format!(
            "a lookback window cannot be negative ({lookback})"
        )));
    }
    Ok(())
}

/// The window `[max(at − lookback, floor), at]` on `ts_column`, `floor`
/// being the smallest value its type holds.
pub(crate) fn lookback_window(
    schema: &Schema,
    ts_column: String,
    at: i64,
    lookback: i64,
) -> Result<RunWindow> {
    check_lookback(lookback)?;
    let floor = encoding::ts_min(&ts_type(schema, &ts_column)?)?;
    Ok(RunWindow {
        ts_column,
        lo: at.saturating_sub(lookback).max(floor),
        hi: at,
    })
}

/// The window `[max(at − lookback, floor), at]`, with `at` and `lookback`
/// in the unit of the column `recorded` (the column the run's instants were
/// measured on), planned on `ts_column`: the converted window holds the
/// `ts_column` value of every instant the recorded one covers, rounding
/// outward ([`instant_span_as`]: a recorded value `v` covers the instants
/// from the start of `v` to the end of `v`, so a coarse `hi` reaches the
/// last fine value inside it). `None` when the two columns' units do not
/// convert (an integer or time-of-day column on either side, unless both
/// columns have the same type): the run has no window on `ts_column`.
/// The one conversion replay prices with and verify compares rows by.
pub(crate) fn converted_lookback_window(
    schema: &Schema,
    recorded: &str,
    ts_column: String,
    at: i64,
    lookback: i64,
) -> Result<Option<RunWindow>> {
    let from = ts_type(schema, recorded)?;
    let to = ts_type(schema, &ts_column)?;
    let RunWindow { lo, hi, .. } = lookback_window(schema, recorded.to_string(), at, lookback)?;
    if from == to {
        return Ok(Some(RunWindow { ts_column, lo, hi }));
    }
    let (Some((lo, _)), Some((_, hi))) = (
        instant_span_as(lo, &from, &to)?,
        instant_span_as(hi, &from, &to)?,
    ) else {
        return Ok(None);
    };
    Ok(Some(RunWindow { ts_column, lo, hi }))
}

/// Nanoseconds per unit of an instant column's values: a timestamp's unit,
/// a day for `Date32`, a millisecond for `Date64`. `None` for any other type
/// (an integer or a time of day is not an instant).
fn nanos_per_unit(dt: &DataType) -> Option<i128> {
    use arrow::datatypes::TimeUnit;
    Some(match dt {
        DataType::Dictionary(_, v) => return nanos_per_unit(v),
        DataType::Timestamp(TimeUnit::Second, _) => 1_000_000_000,
        DataType::Timestamp(TimeUnit::Millisecond, _) | DataType::Date64 => 1_000_000,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 1,
        DataType::Date32 => 86_400 * 1_000_000_000,
        _ => return None,
    })
}

/// The `to` values of every instant the `from` value `value` covers, the
/// first and the last: a value `v` in a unit of `f` nanoseconds covers the
/// instants `[v·f, (v+1)·f − 1]`, which in a unit of `t` nanoseconds are
/// `⌊v·f / t⌋` through `⌊((v+1)·f − 1) / t⌋` (floor division, so instants
/// before 1970 round down too), each clamped to the values `to` holds.
/// `None` when either type is not an instant ([`nanos_per_unit`]).
fn instant_span_as(value: i64, from: &DataType, to: &DataType) -> Result<Option<(i64, i64)>> {
    let (Some(f), Some(t)) = (nanos_per_unit(from), nanos_per_unit(to)) else {
        return Ok(None);
    };
    let max: i128 = match to {
        DataType::Date32 => i32::MAX.into(),
        DataType::Dictionary(_, v) if **v == DataType::Date32 => i32::MAX.into(),
        _ => i64::MAX.into(),
    };
    let min = i128::from(encoding::ts_min(to)?);
    let v = i128::from(value);
    let first = (v * f).div_euclid(t).clamp(min, max);
    let last = ((v + 1) * f - 1).div_euclid(t).clamp(min, max);
    let as_i64 = |x: i128| {
        i64::try_from(x).map_err(|_| Error::Other(format!("instant {x} out of range for {to}")))
    };
    Ok(Some((as_i64(first)?, as_i64(last)?)))
}

/// `query` as a planner plans it under `specs`: an `asof` lookback run is
/// its window ([`asof_lookback_window`]), and an error when no temporal
/// spec serves it; every other query is itself.
pub(crate) fn planned_query<'q>(
    schema: &Schema,
    specs: &[KeySpec],
    run_columns: &[String],
    query: &'q RunQuery,
) -> Result<Cow<'q, RunQuery>> {
    match query {
        RunQuery::Lookback {
            asof: Some(at),
            lookback,
        } => asof_lookback_window(schema, specs, run_columns, *at, *lookback)?
            .map(|w| Cow::Owned(RunQuery::Window(w)))
            .ok_or_else(|| {
                Error::KeySpec(format!(
                    "latest and asof need a temporal key spec serving {run_columns:?}"
                ))
            }),
        q => Ok(Cow::Borrowed(q)),
    }
}

/// The dataset type of the timestamp column `column`.
fn ts_type(schema: &Schema, column: &str) -> Result<DataType> {
    Ok(schema
        .field_with_name(column)
        .map_err(|_| Error::KeySpec(format!("column {column:?} is not in the dataset schema")))?
        .data_type()
        .clone())
}

/// The surface replay plans through. The build crate's planner implements it
/// ([`crate::build_plan`]); [`SegmentPlanner`] implements it over core
/// segments.
///
/// Every plan is computed against one pinned manifest version (invariant
/// 4) and must contain every row group that holds a queried key (invariant
/// 1). Replay checks each returned plan against the catalog
/// ([`Catalog::verify_plan`]) before costing it, so a misreported plan is an
/// error, never a wrong number.
pub trait PlanSource: Send + Sync {
    /// The manifest sequence (ADR 020) every plan is computed against,
    /// rendered as a string.
    fn manifest_version(&self) -> String;

    /// The key specs this source can plan with.
    fn specs(&self) -> Vec<KeySpec>;

    /// The plan for one `locate` run's keys under `specs` (module docs for
    /// the serving, intersection, and no-pruning rules).
    fn plan(&self, specs: &[KeySpec], keys: &RunKeys) -> Result<ScanPlan> {
        self.plan_query(specs, keys, &RunQuery::Locate)
    }

    /// The plan for one run's keys under `specs` and `query` (`locate`,
    /// `latest`, or `asof`).
    fn plan_query(&self, specs: &[KeySpec], keys: &RunKeys, query: &RunQuery) -> Result<ScanPlan>;

    /// The time column a `latest` lookback run on `run_columns` measures
    /// each key's window on under `specs`: the serving temporal spec's
    /// ([`serving_temporal_column`], the default), or another source of each
    /// key's versions the planner has (the build planner's complete per-key
    /// bounds sidecar, `Planner::lookback_column`). `None` when it cannot
    /// plan the run.
    fn lookback_column(&self, specs: &[KeySpec], run_columns: &[String]) -> Result<Option<String>> {
        serving_temporal_column(specs, run_columns)
    }

    /// The time column an `asof` lookback window on `run_columns` takes
    /// when no temporal spec of `specs` serves the run, as a window lookup
    /// with no column named does (the build planner's
    /// `Planner::window_column`). `None`, the default, plans no window.
    fn window_column(&self, _specs: &[KeySpec], _run_columns: &[String]) -> Result<Option<String>> {
        Ok(None)
    }

    /// The plan for the text lookup `op(column, any of values)` under
    /// `specs`: per value, the text keys among them that serve it (core's
    /// `text::Shape`) intersected, every live row group when none prunes;
    /// the values' sets united; residual [`Predicate::Text`]. Specs that are
    /// not text keys take no part. A [`RunQuery::Text`] run plans through
    /// it, its values decoded from its keys.
    fn plan_text(
        &self,
        specs: &[KeySpec],
        column: &str,
        op: TextOp,
        values: &[String],
    ) -> Result<ScanPlan>;

    /// The plan of an aggregate run answered from the index: the row groups
    /// the agg key and the counted key among `specs` ([`aggregate_specs`])
    /// do not answer, with the `In` residual over the keys holding rows
    /// there, and `rows` the keys' exact rows from the counts. Both
    /// planners compute it with `bellwether_build::aggregate_answer`, so a
    /// priced agg key plans what the built one does. An error when no agg
    /// key and counted key among `specs` answer the run.
    fn plan_aggregate(
        &self,
        specs: &[KeySpec],
        keys: &RunKeys,
        aggregate: &RunAggregate,
    ) -> Result<ScanPlan>;

    /// This source with hypothetical specs' transient segments added, so
    /// each plans exactly as committed segments of that spec would. A
    /// transient index covers every live row group of the pinned catalog,
    /// so it is complete by construction.
    fn with_transient(&self, transient: &[Arc<TransientIndex>])
        -> Result<Box<dyn PlanSource + '_>>;
}

/// The build planner's lookup of an aggregate run.
pub(crate) fn agg_lookup(keys: &RunKeys, aggregate: &RunAggregate) -> AggLookup {
    agg_lookup_of(
        keys.columns(),
        keys.tuples().map(<[u8]>::to_vec).collect(),
        aggregate,
    )
}

/// The refusal when no agg key and counted key among `specs` answer an
/// aggregate run: the two declarations it needs.
pub(crate) fn no_aggregate(specs: &[KeySpec], run_columns: &[String], a: &RunAggregate) -> Error {
    let cols = run_columns.join(",");
    Error::KeySpec(format!(
        "none of {:?} answers an aggregate of {} by ({cols}) from the index: it needs an agg key \
         (agg:{cols}@{}{{{}}}) beside a key counting rows on exactly those columns \
         (exact:{cols},counts)",
        specs.iter().map(KeySpec::name).collect::<Vec<_>>(),
        a.value,
        a.value,
        a.functions
            .iter()
            .map(|f| f.as_str())
            .collect::<Vec<_>>()
            .join(",")
    ))
}

/// A [`RunQuery::Text`] run as its text lookup: its one column, its match,
/// and its values decoded from its keys; `None` for any other run.
pub(crate) fn text_run<'k>(
    keys: &'k RunKeys,
    query: &RunQuery,
) -> Result<Option<(&'k str, TextOp, Vec<String>)>> {
    let Some(op) = query.text_op() else {
        return Ok(None);
    };
    let [column] = keys.columns() else {
        return Err(Error::KeySpec(format!(
            "a {} run looks up one string column, and this one looks up {:?}",
            query.kind_str(),
            keys.columns()
        )));
    };
    let values = keys
        .tuples()
        .map(text::arg_of_key)
        .collect::<Result<Vec<_>>>()?;
    Ok(Some((column.as_str(), op, values)))
}

/// The residual of a [`RunQuery::Text`] run (B5): the rows whose string
/// column matches one of the run's values under its match, both sides
/// case-folded, decoded from the workload alone. `None` for any other run.
/// `fix` compares exactly these rows.
pub fn text_residual(keys: &RunKeys, query: &RunQuery) -> Result<Option<Predicate>> {
    Ok(
        text_run(keys, query)?.map(|(column, operator, values)| Predicate::Text {
            column: column.to_string(),
            operator,
            values,
        }),
    )
}

/// The cost of a `count` run ([`RunQuery::Count`]) under its verified
/// plan: nothing read when the plan's rows come from a spec's per-key counts
/// (the index answers), else the planned row groups, which a count reads as
/// a `locate` does. Exact either way (ADR 005).
#[must_use]
pub fn count_cost(run_id: &str, keys: u64, plan: &ScanPlan) -> RunCost {
    let mut cost = run_cost(run_id, keys, plan);
    if plan.rows.is_some() {
        cost.files = 0;
        cost.row_groups = 0;
        cost.bytes = 0;
    }
    cost
}

/// The cost of a run under a verified plan: exact sums (ADR 005), and the
/// rows of the run's keys when a serving spec counts them (`ScanPlan.rows`,
/// exact).
#[must_use]
pub fn run_cost(run_id: &str, keys: u64, plan: &ScanPlan) -> RunCost {
    RunCost {
        measured: None,
        run_id: run_id.to_string(),
        keys,
        files: plan.entries.len() as u64,
        row_groups: plan.row_groups_selected,
        bytes: plan.bytes,
        exact: true,
        exactness: bellwether_types::Exactness::Exact,
        rows: plan.rows.clone(),
    }
}

/// Random-access byte ranges of a stored segment.
pub trait RangeRead: Send + Sync {
    /// Total length in bytes.
    fn len(&self) -> u64;
    /// Whether the segment is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Exactly `len` bytes at `offset`.
    fn read_range(&self, offset: u64, len: usize) -> Result<Bytes>;
}

/// The bytes a segment is read from: memory (transient segments) or byte
/// ranges of a committed segment in a pinned snapshot, so a lookup holds one
/// block index plus the blocks it touches (ADR 018).
#[derive(Clone)]
pub enum SegmentBytes {
    /// The whole segment in memory.
    Memory(Bytes),
    /// Read by range on demand.
    Ranged(Arc<dyn RangeRead>),
}

impl std::fmt::Debug for SegmentBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory(b) => write!(f, "Memory({} bytes)", b.len()),
            Self::Ranged(r) => write!(f, "Ranged({} bytes)", r.len()),
        }
    }
}

impl ReadAt for SegmentBytes {
    fn size(&self) -> Result<u64> {
        match self {
            Self::Memory(b) => Ok(b.len() as u64),
            Self::Ranged(r) => Ok(r.len()),
        }
    }

    fn read_at(&self, offset: u64, len: usize) -> Result<Cow<'_, [u8]>> {
        match self {
            Self::Memory(b) => ReadAt::read_at(b, offset, len),
            Self::Ranged(r) => {
                let bytes = r.read_range(offset, len)?;
                if bytes.len() != len {
                    return Err(Error::Store(format!(
                        "segment range read returned {} of {len} bytes",
                        bytes.len()
                    )));
                }
                Ok(Cow::Owned(bytes.to_vec()))
            }
        }
    }
}

/// A committed segment read by range through a pinned [`Snapshot`], which
/// refuses ids its manifest does not reference (invariant 4).
struct SnapshotSegment<S: StoreExt + ?Sized> {
    snapshot: Snapshot<S>,
    id: Hash,
    len: u64,
}

impl<S: StoreExt + ?Sized + Send + Sync> RangeRead for SnapshotSegment<S> {
    fn len(&self) -> u64 {
        self.len
    }

    fn read_range(&self, offset: u64, len: usize) -> Result<Bytes> {
        let end = offset
            .checked_add(len as u64)
            .filter(|&e| e <= self.len)
            .ok_or_else(|| Error::Store(format!("range past the end of segment {}", self.id)))?;
        self.snapshot.read_segment_range(&self.id, offset..end)
    }
}

/// Every segment of one key spec.
#[derive(Debug)]
pub struct SpecSegments {
    spec: KeySpec,
    readers: Vec<SegmentReader<SegmentBytes>>,
    /// For a spec with a tombstone (B8), its tombstone segments.
    tombstones: Vec<SegmentReader<SegmentBytes>>,
}

impl SpecSegments {
    /// The segments of `spec`. Each must index exactly `spec`.
    pub fn new(spec: KeySpec, readers: Vec<SegmentReader<SegmentBytes>>) -> Result<Self> {
        Self::with_tombstones(spec, readers, Vec::new())
    }

    /// The segments of `spec` and, for a spec with a tombstone (B8), its
    /// tombstone segments. Each of `readers` must index the spec without
    /// its tombstone (its own segments), and each of `tombstones` the spec
    /// with it, in the `keyed_tombstone` layout; a spec with a tombstone
    /// needs its tombstone segments.
    pub fn with_tombstones(
        spec: KeySpec,
        readers: Vec<SegmentReader<SegmentBytes>>,
        tombstones: Vec<SegmentReader<SegmentBytes>>,
    ) -> Result<Self> {
        let own = spec.without_tombstone();
        for r in &readers {
            if r.key_spec() != &own {
                return Err(Error::KeySpec(format!(
                    "a segment indexes {}, not {}",
                    r.key_spec().identity(),
                    own.identity()
                )));
            }
        }
        if spec.tombstone().is_some() && !readers.is_empty() && tombstones.is_empty() {
            return Err(Error::KeySpec(format!(
                "{} has segments and no tombstone segments",
                spec.identity()
            )));
        }
        for r in &tombstones {
            if r.key_spec() != &spec
                || r.header().layout != bellwether_core::segment::Layout::KeyedTombstone
            {
                return Err(Error::KeySpec(format!(
                    "a segment listed as a tombstone segment of {} indexes {}",
                    spec.identity(),
                    r.key_spec().identity()
                )));
            }
        }
        Ok(Self {
            spec,
            readers,
            tombstones,
        })
    }

    /// The key spec.
    #[must_use]
    pub fn spec(&self) -> &KeySpec {
        &self.spec
    }

    /// The segment readers.
    #[must_use]
    pub fn readers(&self) -> &[SegmentReader<SegmentBytes>] {
        &self.readers
    }

    /// The tombstone segment readers (B8); empty for a spec without a
    /// tombstone.
    #[must_use]
    pub fn tombstone_readers(&self) -> &[SegmentReader<SegmentBytes>] {
        &self.tombstones
    }

    /// Row groups that may hold any of `probes` (point probes or prefixes),
    /// unioned across segments.
    fn lookup(&self, probes: &[Vec<u8>], lookup: &Lookup) -> Result<RoaringBitmap> {
        let mut out = RoaringBitmap::new();
        if probes.is_empty() {
            return Ok(out);
        }
        for r in &self.readers {
            match lookup {
                Lookup::Point { .. } => out |= r.lookup(probes)?,
                Lookup::Prefix => {
                    for p in probes {
                        out |= r.lookup_prefix(p)?;
                    }
                }
            }
        }
        Ok(out)
    }
}

/// The in-crate [`PlanSource`] over core segments (module docs).
#[derive(Clone, Debug)]
pub struct SegmentPlanner {
    catalog: Arc<Catalog>,
    specs: Vec<Arc<SpecSegments>>,
    /// Declared specs that are not complete at the pinned sequence, and
    /// why: a plan that lists one is refused.
    incomplete: Vec<(KeySpec, String)>,
    /// The data root and the partitioning its paths are read under, for
    /// window runs on a partition column.
    partitioning: Option<(String, Partitioning)>,
}

impl SegmentPlanner {
    /// A planner with no specs: every plan with keys is a raw scan.
    #[must_use]
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            specs: Vec::new(),
            incomplete: Vec::new(),
            partitioning: None,
        }
    }

    /// This planner reading partition values from the catalog's file paths
    /// under `root` with `scheme`, so a window on a partition column prunes
    /// by path as the build planner does. The catalog's statistics already
    /// carry those values; this only tells the planner which column the
    /// paths bound.
    #[must_use]
    pub fn with_partitioning(mut self, root: &str, scheme: Partitioning) -> Self {
        self.partitioning = (!scheme.is_none()).then(|| (root.to_string(), scheme));
        self
    }

    /// The planner over a pinned snapshot's committed segments: one entry
    /// per declared key spec that is complete at the snapshot's sequence
    /// (ADR 012) with segments whose row-group ranges cover every live row
    /// group. Segments are read by range through the snapshot; each must
    /// index exactly the declared spec and record the dataset schema's key
    /// types, or construction fails. A declared spec that is not complete,
    /// or whose segments leave a gap, is no error here: as in the build
    /// crate's planner, a plan that lists it is refused, and a plan that
    /// does not is unaffected.
    pub fn from_snapshot<S>(snapshot: &Snapshot<S>, catalog: Arc<Catalog>) -> Result<Self>
    where
        S: StoreExt + ?Sized + Send + Sync + 'static,
        Snapshot<S>: Clone,
    {
        let schema = Arc::clone(catalog.schema());
        let manifest = snapshot.manifest();
        let sequence = manifest.sequence;
        if catalog.manifest_version() != sequence.to_string() {
            return Err(Error::Other(format!(
                "catalog is of manifest {} but the snapshot pins {sequence}",
                catalog.manifest_version()
            )));
        }
        let live = catalog.all_ids();
        let mut planner = Self::new(catalog);
        // Each temporal or bitemporal spec with the tombstone its sidecar
        // declares (B8); the manifest's `key_specs` hold them without it.
        let declared = bellwether_build::declared_key_specs(manifest);
        for (i, full) in declared.iter().enumerate() {
            let spec = &full.without_tombstone();
            let name = spec.name();
            if declared[..i].iter().any(|s| s.name() == name) {
                return Err(Error::KeySpec(format!(
                    "two declared key specs share the name {name}; their segments cannot be told \
                     apart"
                )));
            }
            // Segments and completeness are keyed by the spec's identity
            // (storage version 2; a version 1 manifest is re-keyed on read).
            let identity = spec.identity();
            match manifest.key_spec_completeness.get(&identity) {
                Some(&at) if at <= sequence => {}
                _ => {
                    planner.incomplete.push((
                        spec.clone(),
                        format!(
                            "key spec {name} is not complete at manifest sequence {sequence} (ADR \
                             012); a plan on an incomplete key spec is an error, never a partial \
                             answer"
                        ),
                    ));
                    continue;
                }
            }
            let refs: Vec<_> = manifest
                .segments
                .iter()
                .filter(|s| s.kind.is_none() && s.key_spec == identity)
                .collect();
            let mut covered = RoaringBitmap::new();
            for r in &refs {
                if r.rowgroup_range.start.0 < r.rowgroup_range.end.0 {
                    covered.insert_range(r.rowgroup_range.start.0..r.rowgroup_range.end.0);
                }
            }
            let gap = &live - &covered;
            if let Some(first) = gap.min() {
                planner.incomplete.push((
                    spec.clone(),
                    format!(
                        "key spec {name} is marked complete but no segment covers row group \
                         {first} ({} uncovered); refusing a partial plan (ADR 012)",
                        gap.len()
                    ),
                ));
                continue;
            }
            let mut readers = Vec::with_capacity(refs.len());
            for r in refs {
                let bytes = SegmentBytes::Ranged(Arc::new(SnapshotSegment {
                    snapshot: snapshot.clone(),
                    id: r.id,
                    len: r.bytes,
                }));
                readers.push(SegmentReader::open_for_dataset(bytes, &schema)?);
            }
            // A complete tombstone sidecar plans with the spec; an
            // incomplete one is left out, and the spec plans without it (as
            // `bellwether_build::Planner::usable_specs` has it).
            if full.tombstone().is_some()
                && bellwether_build::tombstone_completeness(manifest, full).is_some()
            {
                let sidecar = bellwether_build::tombstone_name(full);
                let mut tombstones = Vec::new();
                for r in manifest.segments.iter().filter(|s| {
                    s.kind.as_deref() == Some(tombstone::TOMBSTONE_KIND) && s.key_spec == sidecar
                }) {
                    let bytes = SegmentBytes::Ranged(Arc::new(SnapshotSegment {
                        snapshot: snapshot.clone(),
                        id: r.id,
                        len: r.bytes,
                    }));
                    tombstones.push(SegmentReader::open_for_dataset(bytes, &schema)?);
                }
                planner.add(SpecSegments::with_tombstones(
                    full.clone(),
                    readers,
                    tombstones,
                )?)?;
            } else {
                planner.add(SpecSegments::new(spec.clone(), readers)?)?;
            }
        }
        Ok(planner)
    }

    /// Adds a spec's segments. A spec already present is an error.
    pub fn add(&mut self, segments: SpecSegments) -> Result<()> {
        self.add_shared(Arc::new(segments))
    }

    fn add_shared(&mut self, segments: Arc<SpecSegments>) -> Result<()> {
        if self.find(&segments.spec).is_some() {
            return Err(Error::KeySpec(format!(
                "key spec {} already has segments in this planner",
                segments.spec.identity()
            )));
        }
        self.specs.push(segments);
        Ok(())
    }

    /// The catalog plans are materialized against.
    #[must_use]
    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    fn find(&self, spec: &KeySpec) -> Option<&Arc<SpecSegments>> {
        self.specs.iter().find(|s| &s.spec == spec)
    }

    /// The segments of a listed spec, or why it cannot be planned with.
    fn listed(&self, spec: &KeySpec) -> Result<&Arc<SpecSegments>> {
        if let Some(s) = self.find(spec) {
            return Ok(s);
        }
        if let Some((_, why)) = self.incomplete.iter().find(|(s, _)| s == spec) {
            return Err(Error::KeySpec(why.clone()));
        }
        Err(Error::KeySpec(format!(
            "no complete segments for {} at manifest {}",
            spec.identity(),
            self.catalog.manifest_version()
        )))
    }

    /// The rows of `keys` in `live` row groups, from the first serving spec
    /// that stores counts and whose key is the whole run tuple, as the build
    /// crate's planner counts them (`ScanPlan.rows`); `None` without one.
    fn count_rows(
        serving: &[(&SpecSegments, Lookup)],
        keys: &RunKeys,
        live: &RoaringBitmap,
    ) -> Result<Option<u64>> {
        let width = keys.columns().len();
        let Some((segments, project)) = serving.iter().find_map(|(s, l)| match l {
            Lookup::Point { project } if s.spec.counts() && project.len() == width => {
                Some((s, project))
            }
            _ => None,
        }) else {
            return Ok(None);
        };
        // Distinct probes: both zeros of a key are two stored keys, so no row
        // counts twice; two segments counting one key in one live row group
        // would (a retired row group is never counted).
        let probes = keys.all_probes(project);
        let mut total = 0u64;
        let mut counted_in: BTreeMap<Vec<u8>, RoaringBitmap> = BTreeMap::new();
        for r in &segments.readers {
            let counted = r.counts(&probes)?.ok_or_else(|| {
                Error::Other(format!(
                    "corrupt segment: a segment of {} stores no counts",
                    segments.spec.identity()
                ))
            })?;
            for (key, row_groups) in &counted {
                let seen = counted_in.entry(key.clone()).or_default();
                for (rg, _) in row_groups {
                    if live.contains(rg.0) && !seen.insert(rg.0) {
                        return Err(bellwether_build::overlapping_counts(&segments.spec));
                    }
                }
            }
            for (rg, n) in counted.into_values().flatten() {
                if live.contains(rg.0) {
                    total = total
                        .checked_add(n)
                        .ok_or_else(|| Error::Other("row count overflows u64".into()))?;
                }
            }
        }
        Ok(Some(total))
    }

    /// The selected row-group ids for `keys` under the serving specs.
    fn select(serving: &[(&SpecSegments, Lookup)], keys: &RunKeys) -> Result<RoaringBitmap> {
        let all: Vec<usize> = (0..keys.columns().len()).collect();
        let project = |lookup: &Lookup| match lookup {
            Lookup::Point { project } => project.clone(),
            Lookup::Prefix => all.clone(),
        };
        if let [(segments, lookup)] = serving {
            return segments.lookup(&keys.all_probes(&project(lookup)), lookup);
        }
        // Per key tuple: intersect what every serving spec returns for it,
        // dropping a tuple as soon as its intersection is empty; then union.
        let mut acc: Vec<Option<RoaringBitmap>> = vec![None; keys.len()];
        let mut alive: Vec<usize> = (0..keys.len()).collect();
        for (segments, lookup) in serving {
            let cols = project(lookup);
            let mut next = Vec::with_capacity(alive.len());
            for i in alive {
                let found = segments.lookup(&keys.probes(i, &cols), lookup)?;
                let merged = match acc[i].take() {
                    None => found,
                    Some(prev) => prev & found,
                };
                if !merged.is_empty() {
                    acc[i] = Some(merged);
                    next.push(i);
                }
            }
            alive = next;
        }
        let mut out = RoaringBitmap::new();
        for i in alive {
            if let Some(b) = &acc[i] {
                out |= b;
            }
        }
        Ok(out)
    }

    /// The selected row-group ids for a window run (module docs): per key
    /// tuple, the intersection of what the specs serving its keys return,
    /// the row groups of its versions inside the window in a temporal spec
    /// on the window's column, the specs serving the window's column, and
    /// the files whose path value lies in the window; then the union.
    fn select_window(
        &self,
        specs: &[KeySpec],
        listed: &[&Arc<SpecSegments>],
        keys: &RunKeys,
        w: &EncodedWindow,
    ) -> Result<RoaringBitmap> {
        let RunWindow { ts_column, lo, hi } = &w.window;
        let by_path = self.path_window(w)?;
        if specs.is_empty() {
            let all = self.catalog.all_ids();
            return Ok(match by_path {
                Some(p) => all & p,
                None => all,
            });
        }
        let all_columns: Vec<usize> = (0..keys.columns().len()).collect();
        let mut serving: Vec<(&SpecSegments, Lookup)> = Vec::new();
        let mut ranged: Option<RoaringBitmap> = None;
        for (spec, segments) in specs.iter().zip(listed) {
            if let Some(lookup) = servable(spec, keys.columns()) {
                serving.push((segments.as_ref(), lookup));
            }
            if serves_window(spec, ts_column) {
                let mut found = RoaringBitmap::new();
                for r in segments.readers() {
                    found |= match spec {
                        KeySpec::Range { .. } => r.lookup_range(&w.lo, &w.hi)?,
                        _ => r.lookup_prefix_range(&w.lo, &w.hi)?,
                    };
                }
                ranged = Some(match ranged {
                    Some(x) => x & found,
                    None => found,
                });
            }
        }
        if serving.is_empty() && ranged.is_none() && by_path.is_none() {
            return Err(Error::KeySpec(format!(
                "none of {:?} serves keys on {:?} in a window of {ts_column:?}",
                specs.iter().map(KeySpec::name).collect::<Vec<_>>(),
                keys.columns()
            )));
        }
        let mut out = RoaringBitmap::new();
        for i in 0..keys.len() {
            let mut acc: Option<RoaringBitmap> = None;
            let mut narrow = |with: RoaringBitmap| {
                acc = Some(match acc.take() {
                    Some(x) => x & with,
                    None => with,
                });
            };
            for (segments, lookup) in &serving {
                let cols = match lookup {
                    Lookup::Point { project } => project.clone(),
                    Lookup::Prefix => all_columns.clone(),
                };
                let probes = keys.probes(i, &cols);
                narrow(segments.lookup(&probes, lookup)?);
                // A temporal spec on the window's column: only the row
                // groups holding a version inside the window.
                if let (KeySpec::Temporal { ts_column: tc, .. }, Lookup::Point { .. }) =
                    (segments.spec(), lookup)
                {
                    if tc == ts_column {
                        let mut inside = RoaringBitmap::new();
                        for r in segments.readers() {
                            for value in r.get(&probes)?.values() {
                                for &(ts, rg) in value.chain.versions() {
                                    if *lo <= ts && ts <= *hi {
                                        inside.insert(rg.0);
                                    }
                                }
                            }
                        }
                        narrow(inside);
                    }
                }
            }
            if let Some(r) = &ranged {
                narrow(r.clone());
            }
            if let Some(p) = &by_path {
                narrow(p.clone());
            }
            if let Some(a) = acc {
                out |= a;
            }
        }
        Ok(out)
    }

    /// The catalog's row groups whose file's path gives the window's column
    /// a value inside it, when the partitioning makes that column a column
    /// of every file; `None` otherwise.
    fn path_window(&self, w: &EncodedWindow) -> Result<Option<RoaringBitmap>> {
        let Some((root, scheme)) = &self.partitioning else {
            return Ok(None);
        };
        let files = self.catalog.files();
        let Some(inside) = bellwether_source::partition::paths_in_window(
            scheme,
            root,
            files.iter().map(|f| f.path.as_str()),
            &w.window.ts_column,
            &w.ty,
            w.window.lo,
            w.window.hi,
        )?
        else {
            return Ok(None);
        };
        let keep: BTreeSet<usize> = inside
            .iter()
            .enumerate()
            .filter_map(|(i, k)| k.then_some(i))
            .collect();
        Ok(Some(
            self.catalog
                .row_groups()
                .iter()
                .filter(|rg| keep.contains(&rg.file))
                .map(|rg| rg.id.0)
                .collect(),
        ))
    }

    /// Plans a `latest`, `asof`, or per-key lookback run over `serving`,
    /// following the build planner's temporal rules
    /// (`bellwether_build::Planner::plan_temporal`): exactly one serving
    /// temporal spec with every column in `keys`, cross-segment winner per
    /// key (recomputed over live row groups when the winner lies in retired
    /// ones, invariant 5) or every live version in the key's own lookback
    /// window, per-key intersection with any other serving specs, and the
    /// `NotOlderThan` / `LatestPerKey` residual, or the lookback's.
    fn plan_temporal(
        &self,
        serving: &[(&SpecSegments, Lookup)],
        keys: &RunKeys,
        query: &RunQuery,
    ) -> Result<ScanPlan> {
        let mut temporal = serving
            .iter()
            .filter(|(s, _)| matches!(s.spec, KeySpec::Temporal { .. }));
        let (t, how) = match (temporal.next(), temporal.next()) {
            (Some((t, how)), None) => (*t, how),
            (None, _) => {
                return Err(Error::KeySpec(format!(
                    "latest and asof need a temporal key spec serving {:?}",
                    keys.columns()
                )))
            }
            (Some(_), Some(_)) => {
                return Err(Error::KeySpec(
                    "more than one temporal key spec serves this lookup; name one".into(),
                ))
            }
        };
        let (Lookup::Point { project }, KeySpec::Temporal { ts_column, .. }) = (how, &t.spec)
        else {
            return Err(Error::KeySpec(format!(
                "latest and asof need every column of {} in the lookup",
                t.spec.name()
            )));
        };
        let ts_type = ts_type(self.catalog.schema(), ts_column)?;
        let variants: Vec<Vec<Vec<u8>>> =
            (0..keys.len()).map(|i| keys.probes(i, project)).collect();
        let asof = query.asof_ts();
        let (selection, mut acc) = match query {
            RunQuery::Lookback {
                asof: None,
                lookback,
            } => {
                let hits = self.lookbacks(&t.readers, &variants, &ts_type, *lookback)?;
                let acc: Vec<Option<RoaringBitmap>> = hits
                    .iter()
                    .map(|w| Some(w.as_ref().map(|h| h.row_groups.clone()).unwrap_or_default()))
                    .collect();
                (Selection::Lookbacks(hits), acc)
            }
            RunQuery::Latest | RunQuery::Asof { .. } | RunQuery::Live(_) => {
                let mut winners = self.winners(&t.readers, &variants, asof)?;
                let mut acc: Vec<Option<RoaringBitmap>> = winners
                    .iter()
                    .map(|w| Some(w.as_ref().map(|h| h.row_groups.clone()).unwrap_or_default()))
                    .collect();
                // With a tombstone (B8), the build planner's rule: a spec
                // declared with exactly the run's tombstone prunes each key's
                // winning row groups by its stored liveness; a key whose
                // every winning row group goes answers no row.
                if let (RunQuery::Live(l), Some(spec_t)) = (query, t.spec.tombstone()) {
                    if *spec_t == l.tombstone {
                        let mut probes: Vec<Vec<u8>> = variants
                            .iter()
                            .zip(&winners)
                            .filter(|(_, w)| w.is_some())
                            .flat_map(|(vs, _)| vs.iter().cloned())
                            .collect();
                        probes.sort_unstable();
                        probes.dedup();
                        let entries = tombstone::liveness(&t.tombstones, &probes)?;
                        for ((vs, w), a) in variants.iter().zip(&mut winners).zip(&mut acc) {
                            let Some(h) = w.as_ref() else { continue };
                            let maps: Vec<&tombstone::KeyLiveness> =
                                vs.iter().filter_map(|v| entries.get(v)).collect();
                            let kept = tombstone::prune(&h.row_groups, (h.ts, 0), &maps)?;
                            if kept.is_empty() {
                                *w = None;
                            }
                            *a = Some(kept);
                        }
                    }
                }
                (Selection::Winners(winners), acc)
            }
            RunQuery::Locate
            | RunQuery::Count
            | RunQuery::Window(_)
            | RunQuery::Text(_)
            | RunQuery::Lookback { asof: Some(_), .. }
            | RunQuery::ContainsAny
            | RunQuery::ContainsAll
            | RunQuery::Bitemporal(_)
            | RunQuery::SemiJoin(_)
            | RunQuery::Aggregate(_) => {
                return Err(Error::Other(format!(
                    "a {} run is not planned through version chains alone",
                    query.kind_str()
                )))
            }
        };
        let all_cols: Vec<usize> = (0..keys.columns().len()).collect();
        for (seg, lookup) in serving {
            if std::ptr::eq(*seg, t) {
                continue;
            }
            let cols = match lookup {
                Lookup::Point { project } => project.as_slice(),
                Lookup::Prefix => all_cols.as_slice(),
            };
            for (i, a) in acc.iter_mut().enumerate() {
                let found = seg.lookup(&keys.probes(i, cols), lookup)?;
                *a = Some(match a.take() {
                    Some(prev) => prev & found,
                    None => found,
                });
            }
        }
        let selected = acc
            .into_iter()
            .flatten()
            .fold(RoaringBitmap::new(), |u, b| u | b);
        let residual = match &selection {
            Selection::Winners(winners) => {
                temporal_residual(&t.spec, ts_column, &ts_type, keys, &variants, winners, asof)?
            }
            Selection::Lookbacks(hits) => {
                lookback_residual(&t.spec, ts_column, &ts_type, keys, &variants, hits)?
            }
        };
        let residual = match query {
            RunQuery::Live(l) => {
                let full = t
                    .spec
                    .without_tombstone()
                    .with_tombstone(l.tombstone.clone())
                    .ok_or_else(|| Error::KeySpec("a live run needs a temporal key".into()))?;
                let dt = tombstone::column_type(&full, self.catalog.schema())?;
                let live = Predicate::Live {
                    column: l.tombstone.column.clone(),
                    tombstone: tombstone::tombstone_bytes(&l.tombstone, &dt)?,
                };
                match residual {
                    Predicate::And { mut all } => {
                        all.push(live);
                        Predicate::And { all }
                    }
                    r => Predicate::And { all: vec![r, live] },
                }
            }
            _ => residual,
        };
        let mut plan = self.catalog.plan_for(&selected)?;
        plan.residual = Some(residual);
        Ok(plan)
    }

    /// Each key's index-wide winning version (latest, or at `asof`), its
    /// zero variants' hits folded into one, as the build planner does.
    fn winners(
        &self,
        readers: &[SegmentReader<SegmentBytes>],
        variants: &[Vec<Vec<u8>>],
        asof: Option<i64>,
    ) -> Result<Vec<Option<VersionHit>>> {
        let mut probes: Vec<Vec<u8>> = variants.iter().flatten().cloned().collect();
        probes.sort_unstable();
        probes.dedup();
        let hits = self.temporal_hits(readers, &probes, asof)?;
        Ok(variants
            .iter()
            .map(|vs| {
                vs.iter()
                    .filter_map(|v| hits.get(v))
                    .fold(None, |acc: Option<VersionHit>, h| {
                        Some(match acc {
                            Some(mut a) => {
                                a.combine(h);
                                a
                            }
                            None => h.clone(),
                        })
                    })
            })
            .collect())
    }

    /// Every row-group id a stored bitmap or chain names must be live or
    /// tombstoned in the pinned manifest.
    fn check_known(&self, bm: &RoaringBitmap) -> Result<()> {
        for id in bm {
            if self.catalog.get(id).is_none() && !self.catalog.is_tombstoned(id) {
                return Err(Error::Other(format!(
                    "row-group id {id} is neither live nor tombstoned in manifest {}",
                    self.catalog.manifest_version()
                )));
            }
        }
        Ok(())
    }

    /// Each key's lookback window, as the build planner computes it
    /// (`bellwether_build::Lookup::Lookback`): from every segment's version
    /// chains, counting only live row groups.
    fn lookbacks(
        &self,
        readers: &[SegmentReader<SegmentBytes>],
        variants: &[Vec<Vec<u8>>],
        ts_type: &DataType,
        lookback: i64,
    ) -> Result<Vec<Option<LookbackHit>>> {
        if lookback < 0 {
            return Err(Error::KeySpec(format!(
                "a lookback window cannot be negative ({lookback})"
            )));
        }
        let mut probes: Vec<&Vec<u8>> = variants.iter().flatten().collect();
        probes.sort_unstable();
        probes.dedup();
        let chains = temporal::chains(readers, &probes)?;
        for chain in chains.values() {
            self.check_known(&chain.versions().iter().map(|(_, rg)| rg.0).collect())?;
        }
        let floor = encoding::ts_min(ts_type)?;
        let live = self.catalog.all_ids();
        Ok(variants
            .iter()
            .map(|vs| {
                temporal::lookback(
                    vs.iter().filter_map(|v| chains.get(v)),
                    &live,
                    lookback,
                    floor,
                )
            })
            .collect())
    }

    fn temporal_hits(
        &self,
        readers: &[SegmentReader<SegmentBytes>],
        probes: &[Vec<u8>],
        asof: Option<i64>,
    ) -> Result<BTreeMap<Vec<u8>, VersionHit>> {
        let raw = match asof {
            None => temporal::latest(readers, probes)?,
            Some(ts) => temporal::asof(readers, probes, ts)?,
        };
        let live = self.catalog.all_ids();
        let check_known = |bm: &RoaringBitmap| self.check_known(bm);
        let mut out = BTreeMap::new();
        let mut redo = Vec::new();
        for (key, mut hit) in raw {
            check_known(&hit.row_groups)?;
            hit.row_groups &= &live;
            if hit.row_groups.is_empty() {
                redo.push(key);
            } else {
                out.insert(key, hit);
            }
        }
        if redo.is_empty() {
            return Ok(out);
        }
        for r in readers {
            for (key, value) in r.get(&redo)? {
                check_known(&value.bitmap)?;
                if let Some(h) = live_winner(&value.chain, &live, asof) {
                    match out.get_mut(&key) {
                        Some(acc) => acc.combine(&h),
                        None => {
                            out.insert(key, h);
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// A membership run (`contains_any`, `contains_all`) on its one list
    /// column, by the build planner's `Lookup::ContainsAny` /
    /// `ContainsAll` rules: the list key on the column unions (any) or
    /// intersects (all) each value's row groups, both encodings of a float
    /// zero counting as one value; with no spec, every live row group; a
    /// listed spec of which none is that list key is an error. No value
    /// plans nothing (`contains_any`); `contains_all` with none is refused.
    fn plan_membership(
        &self,
        specs: &[KeySpec],
        listed: &[&Arc<SpecSegments>],
        keys: &RunKeys,
        all: bool,
    ) -> Result<ScanPlan> {
        let [column] = keys.columns() else {
            return Err(Error::KeySpec(format!(
                "a membership run looks up one list column, not {:?}",
                keys.columns()
            )));
        };
        bellwether_core::list::check_list_column(column, &ts_type(self.catalog.schema(), column)?)?;
        let mut groups: Vec<Vec<Vec<u8>>> = (0..keys.len()).map(|i| keys.probes(i, &[0])).collect();
        groups.sort_unstable();
        groups.dedup();
        if all && groups.is_empty() {
            return Err(Error::KeySpec(format!(
                "contains_all on {column} needs at least one value: with none, every row \
                 matches, null and empty lists included, which a list key does not record; \
                 pass a key file holding the values, or read the folder without a lookup"
            )));
        }
        let query = if all {
            RunQuery::ContainsAll
        } else {
            RunQuery::ContainsAny
        };
        let serving: Vec<&Arc<SpecSegments>> = specs
            .iter()
            .zip(listed)
            .filter(|(s, _)| serves_query(s, keys.columns(), &query))
            .map(|(_, l)| *l)
            .collect();
        if !specs.is_empty() && serving.is_empty() {
            return Err(Error::KeySpec(format!(
                "none of {:?} serves a membership run on {column}; only a list key does",
                specs.iter().map(KeySpec::name).collect::<Vec<_>>()
            )));
        }
        let selected = if groups.is_empty() {
            RoaringBitmap::new()
        } else if serving.is_empty() {
            self.catalog.all_ids()
        } else {
            let probes = keys.all_probes(&[0]);
            let mut per: BTreeMap<Vec<u8>, RoaringBitmap> = BTreeMap::new();
            for s in &serving {
                for (k, b) in bellwether_core::list::element_row_groups(s.readers(), &probes)? {
                    *per.entry(k).or_default() |= b;
                }
            }
            let of = |g: &Vec<Vec<u8>>| {
                g.iter()
                    .filter_map(|v| per.get(v))
                    .fold(RoaringBitmap::new(), |u, b| u | b)
            };
            if all {
                let mut acc: Option<RoaringBitmap> = None;
                for g in &groups {
                    let b = of(g);
                    acc = Some(match acc {
                        Some(a) => a & b,
                        None => b,
                    });
                }
                acc.unwrap_or_default()
            } else {
                groups.iter().fold(RoaringBitmap::new(), |u, g| u | of(g))
            }
        };
        let mut plan = self.catalog.plan_for(&selected)?;
        plan.residual = Some(membership_residual(column, keys, all));
        Ok(plan)
    }
}

impl PlanSource for SegmentPlanner {
    fn manifest_version(&self) -> String {
        self.catalog.manifest_version().to_string()
    }

    fn specs(&self) -> Vec<KeySpec> {
        self.specs.iter().map(|s| s.spec.clone()).collect()
    }

    #[allow(clippy::too_many_lines)]
    fn plan_query(&self, specs: &[KeySpec], keys: &RunKeys, query: &RunQuery) -> Result<ScanPlan> {
        check_key_types(self.catalog.schema(), keys)?;
        if let Some((column, op, values)) = text_run(keys, query)? {
            return self.plan_text(specs, column, op, &values);
        }
        // Every listed spec must be plannable, whether or not it serves
        // these keys, as in the build crate's planner.
        let mut listed = Vec::with_capacity(specs.len());
        for (i, spec) in specs.iter().enumerate() {
            if specs[..i].iter().any(|s| s.name() == spec.name()) {
                return Err(Error::KeySpec(format!(
                    "key spec {} is listed twice",
                    spec.name()
                )));
            }
            listed.push(self.listed(spec)?);
        }
        let query = planned_query(self.catalog.schema(), specs, keys.columns(), query)?;
        if query.bitemporal().is_some() && !specs.is_empty() {
            // Bitemporal versions are planned by the build planner over a
            // committed index (its bitemporal segments); this planner plans
            // over transient key segments only.
            let spec = query.bitemporal().map(|b| {
                format!(
                    "temporal2:{}@{},{}{}",
                    keys.columns().join(","),
                    b.valid_column,
                    b.known_column,
                    b.tombstone
                        .as_ref()
                        .map_or_else(String::new, |t| format!(",{}", t.spelling()))
                )
            });
            return Err(Error::KeySpec(format!(
                "a bitemporal run on {} is priced over an index of its bitemporal key: run \
                 `bellwether index` on the data folder with --key {}, then price it again",
                keys.columns().join(", "),
                spec.unwrap_or_default()
            )));
        }
        if query.is_membership() {
            let all = matches!(*query, RunQuery::ContainsAll);
            return self.plan_membership(specs, &listed, keys, all);
        }
        if *query != RunQuery::Locate && is_expression_run(self.catalog.schema(), keys.columns()) {
            return Err(Error::KeySpec(format!(
                "{} is an expression; an expression key answers point lookups (locate) only, \
                 so a latest, asof, lookback, or window run needs dataset columns",
                keys.columns().join(", ")
            )));
        }
        if query.is_temporal() && specs.is_empty() {
            return Err(Error::KeySpec(format!(
                "latest and asof need a temporal key spec serving {:?}",
                keys.columns()
            )));
        }
        let window = query
            .window()
            .map(|w| EncodedWindow::new(self.catalog.schema(), w))
            .transpose()?;
        if !query.is_temporal() {
            if keys.is_empty() {
                return Ok(with_residual(
                    self.catalog.plan_for(&RoaringBitmap::new())?,
                    self.catalog.schema(),
                    keys,
                    window.as_ref(),
                ));
            }
            if let Some(w) = &window {
                let selected = self.select_window(specs, &listed, keys, w)?;
                return Ok(with_residual(
                    self.catalog.plan_for(&selected)?,
                    self.catalog.schema(),
                    keys,
                    Some(w),
                ));
            }
            if specs.is_empty() {
                return Ok(with_residual(
                    self.catalog.plan_for(&self.catalog.all_ids())?,
                    self.catalog.schema(),
                    keys,
                    None,
                ));
            }
        }
        let mut serving = Vec::with_capacity(specs.len());
        for (spec, segments) in specs.iter().zip(listed) {
            if let Some(lookup) = servable(spec, keys.columns()) {
                serving.push((segments.as_ref(), lookup));
            }
        }
        if serving.is_empty() {
            return Err(Error::KeySpec(format!(
                "none of {:?} serves keys on {:?}",
                specs.iter().map(KeySpec::name).collect::<Vec<_>>(),
                keys.columns()
            )));
        }
        if query.is_temporal() {
            return self.plan_temporal(&serving, keys, &query);
        }
        let selected = Self::select(&serving, keys)?;
        let rows = Self::count_rows(&serving, keys, &self.catalog.all_ids())?;
        let mut plan = with_residual(
            self.catalog.plan_for(&selected)?,
            self.catalog.schema(),
            keys,
            None,
        );
        plan.rows = rows.map(Quantity::exact);
        Ok(plan)
    }

    fn plan_text(
        &self,
        specs: &[KeySpec],
        column: &str,
        op: TextOp,
        values: &[String],
    ) -> Result<ScanPlan> {
        let schema = self.catalog.schema();
        let dt = schema
            .field_with_name(column)
            .map_err(|_| Error::KeySpec(encoding::not_in_schema(schema, column)))?
            .data_type();
        text::check_column_type(column, dt)?;
        let mut selected = RoaringBitmap::new();
        for value in values {
            let shape = text::Shape::of(specs, column, op, value)?;
            let mut acc: Option<RoaringBitmap> = None;
            for (spec, probe) in &shape.serving {
                let segments = self.listed(spec)?;
                if let Some(b) = text::select(segments.readers(), probe)? {
                    acc = Some(match acc {
                        Some(a) => a & b,
                        None => b,
                    });
                }
            }
            selected |= acc.unwrap_or_else(|| self.catalog.all_ids());
        }
        let mut plan = self.catalog.plan_for(&selected)?;
        plan.residual = Some(Predicate::Text {
            column: column.to_string(),
            operator: op,
            values: values.to_vec(),
        });
        Ok(plan)
    }

    fn plan_aggregate(
        &self,
        specs: &[KeySpec],
        keys: &RunKeys,
        aggregate: &RunAggregate,
    ) -> Result<ScanPlan> {
        let schema = self.catalog.schema();
        check_key_types(schema, keys)?;
        let (agg, counting) = aggregate_specs(specs, keys.columns(), aggregate)
            .ok_or_else(|| no_aggregate(specs, keys.columns(), aggregate))?;
        let (a, c) = (self.listed(&agg)?, self.listed(&counting)?);
        let types = keys
            .columns()
            .iter()
            .map(|col| Ok((col.clone(), crate::keys::declared_type(schema, col)?)))
            .collect::<Result<Vec<_>>>()?;
        let lookup = agg_lookup(keys, aggregate);
        let answer = bellwether_build::aggregate_answer(
            (&agg, a.readers()),
            (&counting, c.readers()),
            &self.catalog.all_ids(),
            &types,
            &lookup,
        )?;
        let mut plan = self.catalog.plan_for(&answer.residual)?;
        plan.residual = Some(answer.residual_predicate(&lookup));
        plan.rows = Some(Quantity::exact(answer.rows));
        Ok(plan)
    }

    fn with_transient(
        &self,
        transient: &[Arc<TransientIndex>],
    ) -> Result<Box<dyn PlanSource + '_>> {
        let mut p = self.clone();
        for t in transient {
            if t.manifest_version() != self.catalog.manifest_version()
                || t.catalog_digest() != self.catalog.digest()
            {
                return Err(Error::Other(format!(
                    "transient index for {} was built over another row-group set (manifest {}, \
                     catalog {})",
                    t.spec().name(),
                    t.manifest_version(),
                    t.catalog_digest()
                )));
            }
            // Keys encoded under another type (decimal scale, time unit)
            // would silently miss: every segment must record exactly the
            // dataset types this planner probes with.
            for r in t.segments().readers() {
                r.check_dataset(self.catalog.schema())?;
            }
            p.add_shared(t.segments())?;
        }
        Ok(Box::new(p))
    }
}

/// Refuses keys recorded under another type than the dataset declares for
/// their columns: probes encoded at another decimal scale or time unit
/// would silently miss.
pub(crate) fn check_key_types(schema: &Schema, keys: &RunKeys) -> Result<()> {
    for (c, recorded) in keys.columns().iter().zip(keys.key_types()) {
        let declared = crate::keys::declared_type(schema, c)?;
        if ColumnType::of(bellwether_core::list::value_type(&declared))? != *recorded {
            return Err(Error::KeySpec(format!(
                "keys on {c:?} were encoded as {recorded}, but the dataset declares {declared}; re-read the keys"
            )));
        }
    }
    Ok(())
}

/// Whether a run's key columns are one expression (`bellwether_core::expr`)
/// rather than dataset columns.
pub(crate) fn is_expression_run(schema: &Schema, columns: &[String]) -> bool {
    matches!(columns, [c] if bellwether_core::expr::is_expression(c, schema))
}

/// A run's window with both bounds encoded at the time column's dataset
/// type.
pub(crate) struct EncodedWindow {
    pub(crate) window: RunWindow,
    pub(crate) ty: DataType,
    pub(crate) lo: Vec<u8>,
    pub(crate) hi: Vec<u8>,
}

impl EncodedWindow {
    /// `window` at `schema`'s type for its time column, which must be a
    /// column a time window can bound (a timestamp, date, time, or
    /// integer).
    pub(crate) fn new(schema: &Schema, window: &RunWindow) -> Result<Self> {
        let ty = schema
            .field_with_name(&window.ts_column)
            .map_err(|_| {
                Error::KeySpec(format!(
                    "the window's time column {:?} is not in the dataset",
                    window.ts_column
                ))
            })?
            .data_type()
            .clone();
        bellwether_core::encoding::check_ts_type(&ty)?;
        Ok(Self {
            lo: encode_ts(window.lo, &ty)?,
            hi: encode_ts(window.hi, &ty)?,
            window: window.clone(),
            ty,
        })
    }
}

/// The residual of a membership run on list column `column` (ADR 011), as
/// the build planner writes it: `ContainsAny` over every value's encodings
/// (both zeros), or `ContainsAll` with one sorted group per value. Also the
/// rows a membership run asks for, which `fix` compares.
#[must_use]
pub fn membership_residual(column: &str, keys: &RunKeys, all: bool) -> Predicate {
    if all {
        let mut groups: Vec<Vec<Vec<u8>>> = (0..keys.len()).map(|i| keys.probes(i, &[0])).collect();
        groups.sort_unstable();
        groups.dedup();
        Predicate::ContainsAll {
            column: column.to_string(),
            values: groups,
        }
    } else {
        Predicate::ContainsAny {
            column: column.to_string(),
            values: keys.all_probes(&[0]),
        }
    }
}

/// `plan` with the residual that makes it row-exact (ADR 011): the run's key
/// tuples, float zeros in both encodings, and for a window run `Between` on
/// its time column. An empty key set gives an empty `In` and no row groups.
/// A run keyed on an expression gets `ExprIn`, which recomputes it per row.
pub(crate) fn with_residual(
    mut plan: ScanPlan,
    schema: &Schema,
    keys: &RunKeys,
    window: Option<&EncodedWindow>,
) -> ScanPlan {
    let all: Vec<usize> = (0..keys.columns().len()).collect();
    let keyed = if is_expression_run(schema, keys.columns()) {
        Predicate::ExprIn {
            expr: keys.columns()[0].clone(),
            values: keys.all_probes(&all),
        }
    } else {
        Predicate::In {
            columns: keys.columns().to_vec(),
            values: keys.all_probes(&all),
        }
    };
    plan.residual = Some(match window {
        None => keyed,
        Some(w) => Predicate::And {
            all: vec![
                keyed,
                Predicate::Between {
                    column: w.window.ts_column.clone(),
                    lo: w.lo.clone(),
                    hi: w.hi.clone(),
                },
            ],
        },
    });
    plan
}

fn live_winner(
    chain: &VersionChain,
    live: &RoaringBitmap,
    asof: Option<i64>,
) -> Option<VersionHit> {
    let mut best: Option<VersionHit> = None;
    for &(ts, rg) in chain.versions() {
        if !live.contains(rg.0) || asof.is_some_and(|a| ts > a) {
            continue;
        }
        let hit = VersionHit {
            ts,
            row_groups: std::iter::once(rg.0).collect(),
        };
        match &mut best {
            Some(b) => b.combine(&hit),
            None => best = Some(hit),
        }
    }
    best
}

/// Per key, what a temporal run selected: its winning version, or its
/// lookback window.
enum Selection {
    Winners(Vec<Option<VersionHit>>),
    Lookbacks(Vec<Option<LookbackHit>>),
}

/// The residual of a per-key lookback run, as the build planner writes it:
/// `In` over the keys with a live version, then `NotOlderThan` with each
/// key's `newest − lookback`.
fn lookback_residual(
    spec: &KeySpec,
    ts_column: &str,
    ts_type: &DataType,
    keys: &RunKeys,
    variants: &[Vec<Vec<u8>>],
    hits: &[Option<LookbackHit>],
) -> Result<Predicate> {
    let all_cols: Vec<usize> = (0..keys.columns().len()).collect();
    let mut in_values: Vec<Vec<u8>> = (0..keys.len())
        .zip(hits)
        .filter(|(_, w)| w.is_some())
        .flat_map(|(i, _)| keys.probes(i, &all_cols))
        .collect();
    in_values.sort_unstable();
    in_values.dedup();
    let mut bounds: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for (vs, w) in variants.iter().zip(hits) {
        let Some(w) = w else { continue };
        let lo = encode_ts(w.lo, ts_type)?;
        for v in vs {
            bounds.insert(v.clone(), lo.clone());
        }
    }
    Ok(Predicate::And {
        all: vec![
            Predicate::In {
                columns: keys.columns().to_vec(),
                values: in_values,
            },
            Predicate::NotOlderThan {
                columns: spec.columns().to_vec(),
                ts_column: ts_column.to_string(),
                bounds: bounds.into_iter().collect(),
            },
        ],
    })
}

fn temporal_residual(
    spec: &KeySpec,
    ts_column: &str,
    ts_type: &DataType,
    keys: &RunKeys,
    variants: &[Vec<Vec<u8>>],
    winners: &[Option<VersionHit>],
    asof: Option<i64>,
) -> Result<Predicate> {
    let all_cols: Vec<usize> = (0..keys.columns().len()).collect();
    let mut in_values: Vec<Vec<u8>> = (0..keys.len())
        .zip(winners)
        .filter(|(_, w)| w.is_some())
        .flat_map(|(i, _)| keys.probes(i, &all_cols))
        .collect();
    in_values.sort_unstable();
    in_values.dedup();
    let mut bounds: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut lowest: Option<i64> = None;
    for (vs, w) in variants.iter().zip(winners) {
        let Some(h) = w else { continue };
        lowest = Some(lowest.map_or(h.ts, |l| l.min(h.ts)));
        let ts = encode_ts(h.ts, ts_type)?;
        for v in vs {
            bounds.insert(v.clone(), ts.clone());
        }
    }
    let mut all = vec![Predicate::In {
        columns: keys.columns().to_vec(),
        values: in_values,
    }];
    if let (Some(at), Some(lo)) = (asof, lowest) {
        all.push(Predicate::Between {
            column: ts_column.to_string(),
            lo: encode_ts(lo, ts_type)?,
            hi: encode_ts(at, ts_type)?,
        });
    }
    all.push(Predicate::NotOlderThan {
        columns: spec.columns().to_vec(),
        ts_column: ts_column.to_string(),
        bounds: bounds.into_iter().collect(),
    });
    all.push(Predicate::LatestPerKey {
        columns: spec.columns().to_vec(),
        ts_column: ts_column.to_string(),
    });
    Ok(Predicate::And { all })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(c: &[&str]) -> Vec<String> {
        c.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn serving_follows_the_planner_rules() {
        let exact = |c: &[&str]| KeySpec::Exact {
            columns: cols(c),
            counts: false,
            bounds: None,
        };
        // (a) every spec column in the run's columns: projection, spec order.
        assert_eq!(
            servable(&exact(&["b", "a"]), &cols(&["a", "b"])),
            Some(Lookup::Point {
                project: vec![1, 0]
            })
        );
        assert_eq!(
            servable(&exact(&["a"]), &cols(&["a", "b"])),
            Some(Lookup::Point { project: vec![0] })
        );
        // (b) the run's columns a strict prefix of an exact/temporal spec.
        assert_eq!(
            servable(&exact(&["a", "b"]), &cols(&["a"])),
            Some(Lookup::Prefix)
        );
        let temporal = KeySpec::Temporal {
            columns: cols(&["a", "b"]),
            ts_column: "t".into(),
            counts: false,
            tombstone: None,
        };
        assert_eq!(servable(&temporal, &cols(&["a"])), Some(Lookup::Prefix));
        // Neither: a leading column missing, or a bloom spec asked a prefix.
        assert_eq!(servable(&exact(&["b", "a"]), &cols(&["a"])), None);
        assert_eq!(servable(&exact(&["a", "b"]), &cols(&["a", "c"])), None);
        let bloom = KeySpec::Bloom {
            columns: cols(&["a", "b"]),
            fpr: 0.01,
            bounds: None,
        };
        assert_eq!(servable(&bloom, &cols(&["a"])), None);
        let range = KeySpec::Range { column: "a".into() };
        assert_eq!(
            servable(&range, &cols(&["b", "a"])),
            Some(Lookup::Point { project: vec![1] })
        );
        assert_eq!(servable(&range, &cols(&["b"])), None);
        assert_eq!(servable(&range, &[]), None);
        // A list key serves membership runs only (`serves_query`), never a
        // lookup on the list column's value as a whole.
        let list = KeySpec::List { column: "a".into() };
        assert_eq!(servable(&list, &cols(&["a"])), None);
        assert!(serves_query(&list, &cols(&["a"]), &RunQuery::ContainsAny));
        assert!(!serves_query(&list, &cols(&["a"]), &RunQuery::Locate));
    }

    /// An `asof` lookback measured on a microsecond timestamp and planned
    /// on a `Date32` column takes the days holding its instants, never the
    /// microseconds read as days (which do not fit `Date32`).
    #[test]
    fn a_lookback_window_converts_to_the_planned_columns_unit() {
        use arrow::datatypes::{Field, TimeUnit};
        let schema = Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("day", DataType::Date32, true),
            Field::new("ms", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("v", DataType::Int64, true),
        ]);
        let us_per_day = 86_400_000_000_i64;
        let at = 20_516 * us_per_day + 1;
        let w = |to: &str| converted_lookback_window(&schema, "ts", to.into(), at, 2 * us_per_day);
        let day = w("day").unwrap().unwrap();
        assert_eq!((day.lo, day.hi), (20_514, 20_516));
        let ms = w("ms").unwrap().unwrap();
        assert_eq!((ms.lo, ms.hi), (20_514 * 86_400_000, 20_516 * 86_400_000));
        let same = w("ts").unwrap().unwrap();
        assert_eq!((same.lo, same.hi), (at - 2 * us_per_day, at));
        // An integer column is no instant: no window, not a wrong one.
        assert_eq!(w("v").unwrap(), None);
        // A day back to microseconds covers the whole day: its first
        // microsecond through its last, before the epoch too.
        let back = converted_lookback_window(&schema, "day", "ts".into(), -1, 0)
            .unwrap()
            .unwrap();
        assert_eq!((back.lo, back.hi), (-us_per_day, -1));
        let day_to_us = converted_lookback_window(&schema, "day", "ts".into(), 20_516, 1)
            .unwrap()
            .unwrap();
        assert_eq!(
            (day_to_us.lo, day_to_us.hi),
            (20_515 * us_per_day, 20_517 * us_per_day - 1)
        );
    }

    /// A recorded lookback window converted to the planned column's unit
    /// holds the planned-unit value of every instant the recorded window
    /// covers (a recorded value `v` in a unit of `f` ns covers the instants
    /// `[v·f, (v+1)·f)`), for every pair of instant units, against a
    /// brute-force oracle (the PR #30 review's probe).
    #[test]
    fn a_converted_lookback_window_rounds_outward_for_every_unit_pair() {
        use arrow::datatypes::{Field, TimeUnit};
        let units: Vec<(&str, DataType, i128)> = vec![
            (
                "s",
                DataType::Timestamp(TimeUnit::Second, None),
                1_000_000_000,
            ),
            (
                "ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                1_000_000,
            ),
            (
                "us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                1_000,
            ),
            ("ns", DataType::Timestamp(TimeUnit::Nanosecond, None), 1),
            ("d32", DataType::Date32, 86_400_000_000_000),
            ("d64", DataType::Date64, 1_000_000),
        ];
        let schema = Schema::new(
            units
                .iter()
                .map(|(n, t, _)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        );
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let day_ns: i128 = 86_400_000_000_000;
        let mut failures = Vec::new();
        for (from, _, f) in &units {
            for (to, _, t) in &units {
                // Recorded instants within 60 days of 2026-01-01, and before
                // the epoch, in `from` units.
                for base_day in [20_454_i128, -400] {
                    for _ in 0..200 {
                        let at_ns = (base_day - 60) * day_ns + i128::from(next()) % (120 * day_ns);
                        let at = i64::try_from(at_ns.div_euclid(*f)).unwrap();
                        let lookback =
                            i64::try_from((i128::from(next()) % (3 * day_ns)).div_euclid(*f))
                                .unwrap();
                        let w = converted_lookback_window(
                            &schema,
                            from,
                            (*to).to_string(),
                            at,
                            lookback,
                        )
                        .unwrap()
                        .unwrap();
                        let (lo, hi) = (i128::from(at - lookback), i128::from(at));
                        // The instants the recorded window covers, at its
                        // ends and inside its last value.
                        for x in [lo * f, (hi + 1) * f - 1, hi * f + f / 2] {
                            let v = x.div_euclid(*t);
                            if v < i128::from(w.lo) || v > i128::from(w.hi) {
                                failures.push(format!(
                                    "{from}->{to}: recorded [{lo}, {hi}] -> [{}, {}] misses \
                                     instant {x} ns (value {v})",
                                    w.lo, w.hi
                                ));
                            }
                        }
                        // And it reaches no further than those instants.
                        assert_eq!(i128::from(w.lo), (lo * f).div_euclid(*t), "{from}->{to}");
                        assert_eq!(
                            i128::from(w.hi),
                            ((hi + 1) * f - 1).div_euclid(*t),
                            "{from}->{to}"
                        );
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
