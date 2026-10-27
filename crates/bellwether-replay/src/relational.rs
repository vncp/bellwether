//! Relational replay (WP15, ADR 026): a consumer's query as a typed plan
//! DAG ([`ConsumerPlan`], never SQL), priced node by node from the index
//! and footer statistics alone, under one pinned manifest per dataset.
//!
//! # Input
//!
//! A plan names its datasets; `main` is the replayer's own, and every other
//! is a replayer attached by name ([`Replayer::with_datasets`]). A plan is
//! checked before anything is priced ([`check_shape`], then
//! [`Replayer::check_plan`]): version 1; the root and every input in range;
//! acyclic; every node read by the root; every dataset bound; every column
//! an output of the node's input, by one name; key, join, range, and time
//! columns of a scalar type; join and key columns whose canonical encodings
//! compare (core's `join::check_same_domain`); literal key tuples well
//! formed. A refusal is a typed [`ReplayError::InvalidPlan`] naming the
//! node.
//!
//! # Shared nodes: the plan is a DAG
//!
//! A node may feed several nodes (a diamond, a self-join `join(n, n)`, a
//! subplan two branches read, a node whose output keys a semi-join beside
//! another consumer). The convention is **logical reuse**: every node is
//! evaluated once per run, and its output is what each consumer reads.
//! It is a statement about the plan, not a claim that an engine buffers or
//! reuses anything; an engine that recomputes a shared subplan reads more,
//! which an adapter measures.
//!
//! - **Read occurrences.** Each `scan` node is one read occurrence, priced
//!   once: its columns are the union of the columns every path above it
//!   references, and its row groups the union of the row groups every
//!   consumer path plans from it. Two `scan` nodes of one dataset are two
//!   read occurrences, each priced (a self-join over two scans reads the
//!   dataset twice, as the plan says).
//! - **Selections stay per path.** The union is for reads only. Each
//!   consumer prices its rows from the shared node's own output and its own
//!   narrowing (its keys, its range, its join's common keys), so a sibling
//!   branch's row groups never widen another branch's rows or a join's
//!   multiplicities.
//! - **Bytes per node** are the reads of the node's own sub-DAG, each read
//!   occurrence below it once with the union of the row groups its paths
//!   within that sub-DAG plan.
//! - **Buffering is not priced.** Holding a shared node's output for its
//!   consumers is memory, not storage I/O, and stays unknown until
//!   measured. [`PricedPlan::shared`] lists each shared node with its
//!   consumers and a compressed-equivalent **model estimate** of its output
//!   (`rows_out` × the average compressed row width of the planned row
//!   groups), labelled so: it bounds neither runtime memory nor RSS.
//!
//! # Quantities
//!
//! Every node yields `bytes` and `rows_out` as a [`Quantity`]. Each is an
//! interval `[low, high]` that contains the truth: `Exact` when the two
//! ends meet, `Bounded` otherwise, `Measured` (naming the model) when the
//! node reads a dataset whose statistics a model measured (a SQL table,
//! ADR 027). Rows are proved per node; nothing is estimated (ADR 026).
//!
//! - **bytes** is logical planned I/O: the compressed bytes of the columns
//!   the plan needs (the union of the columns every node above a scan
//!   references, and the root's outputs; a text chunk is read whole) over
//!   the row groups the plan reads at that node, each read occurrence below
//!   it once. It is not wall time and not what an engine reads (an engine
//!   may prune more or read more, which an adapter measures). For an
//!   `exchange` it is compressed-equivalent bytes moved (below).
//! - **rows_out** is the node's output cardinality. Per-key counts give
//!   exact numbers only where the relation they count is proved: a scan's
//!   rows filtered by whole keys (`in`, a semi-join, a window prefilter)
//!   on exactly the counted columns. After a range filter, a `latest` or
//!   `asof`, or a selection on other columns, the index's counts count
//!   rows the relation may not hold, so they bound it from above only.
//!
//! The run's [`RunCost`] is the root's read bytes (an `exchange` moves,
//! it does not read) with the row groups and files every scan reads, and
//! `rows` the root's `rows_out`.
//!
//! # Rules per operator
//!
//! - `scan`: every live row group; rows exact (footer sums).
//! - `filter` with `in` or a semi-join predicate on the run's keys or
//!   literal keys, `semi_join` with the same, and `window` with keys: the
//!   row groups the serving exact, bloom, or temporal key plans (the same
//!   planner a lookup uses; a run no key serves is planned under the
//!   replayer's unserved baseline), intersected with the input's. On a
//!   scan's rows: exact rows from a counted key on exactly those columns,
//!   else `[(key, row group) pairs an exact or temporal key holds, rows of
//!   the planned row groups]`; on any other input `[0, ...]`.
//! - `filter` with `between`: the row groups a range key, else the footer
//!   min/max, keeps; rows `[0, rows of those row groups]`.
//! - `latest`, `as_of` over a scan's rows selected by whole keys on its key
//!   columns, with a temporal key on those columns and `ts`: the winners'
//!   row groups; rows `[keys with a live version (at or before the run's
//!   instant), counted rows of the keys in those row groups or their
//!   rows]`. Duplicates at a winning timestamp are not distinguishable
//!   without per-(key, timestamp) counts, which the format does not store.
//!   Over anything else the node reads its input unchanged, rows `[0,
//!   input]`.
//! - `join`, and a semi-join or window keyed by another node's output: both
//!   sides need a complete exact or temporal key on exactly their join
//!   columns (else [`ReplayError::JoinNotIndexed`]); the common keys come
//!   from a merge of both indexes' sorted key blocks (core's
//!   [`MergedKeys`]), restricted to a side's whole-key selection when one
//!   is below, or from probing that selection; an inner join reads on each
//!   side only the row groups holding a common key, a left join every left
//!   row group. Rows of an inner join are `Σ count_L × count_R`, exact when
//!   both sides carry counts and both are scans' rows selected only by
//!   whole keys on the join columns; a left join adds the unmatched left
//!   rows. Otherwise the interval is bounded by the common keys (when both
//!   sides are proved) and the product of the sides' rows.
//! - A join side (or a semi-join's key source) that is rows derived from
//!   several scans, a join's or an aggregate's output, is matched through
//!   the one scan its join columns come from, which needs the key as
//!   above (columns from two scans are refused). That scan's keys in the
//!   row groups read below hold every key the derived rows carry, so they
//!   narrow the other side's read soundly; the derived side's own read
//!   stays as priced below, and the rows are `[0, derived × other]` (a
//!   left join `[left low, left high + matched]`): never exact, since the
//!   index counts scan rows, not derived ones.
//! - `aggregate`: reads its input; rows the number of groups: exact from an
//!   exact or temporal key on the grouping columns over a scan's rows (its
//!   distinct keys in the planned row groups, plus the null group the
//!   footers' null counts prove), else `[min(1, low), high]`; one row with
//!   no grouping columns.
//! - `exchange`: rows unchanged; bytes are compressed-equivalent bytes
//!   moved: the rows (an interval) times the average compressed row width
//!   over the planned row groups. `Bounded` holds with respect to that
//!   definition; the width models the in-memory width, which no statistic
//!   gives, so it is not a bound on network or memory bytes.
//! - `project`: rows and reads unchanged (projection is pushed into scans).
//!
//! A node above narrows a scan's read (drops row groups from it) only
//! where the rows it drops cannot change a result below: after a `latest`
//! or `as_of` (proved by a temporal key or not), only by whole keys on its
//! key columns; after a `window`, only by whole partitions; otherwise
//! freely. A selection that may not narrow the read still bounds the rows
//! by the row groups it plans.
//!
//! # Memory (ADR 018)
//!
//! One run's key set, the plan, per node its planned row-group bitmap, and
//! for a join one key block per segment of each side.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bellwether_core::encoding;
use bellwether_core::segment::{ColumnType, KeyValue};
use bellwether_core::MergedKeys;
use bellwether_types::{
    ConsumerPlan, Error, Exactness, Hash, JoinKind, KeySpec, KeysFrom, OperatorCost, PlanNode,
    Predicate, Quantity, Result, RunCost,
};
use roaring::RoaringBitmap;

use crate::error::{ReplayError, ReplayResult};
use crate::keys::RunKeys;
use crate::plan::{PlanSource, SpecSegments};
use crate::replay::Replayer;
use crate::workload::RunQuery;

/// The name a plan gives the replayer's own dataset.
pub const MAIN_DATASET: &str = "main";

/// The consumer plan format version this build prices.
pub const PLAN_VERSION: u32 = 1;

fn invalid(node: Option<u32>, reason: impl Into<String>) -> ReplayError {
    ReplayError::InvalidPlan {
        node,
        reason: reason.into(),
    }
}

/// The operator's name as [`PlanNode`] tags it in JSON (`scan`,
/// `semi_join`, `as_of`, ...).
#[must_use]
pub fn op_name(node: &PlanNode) -> &'static str {
    match node {
        PlanNode::Scan { .. } => "scan",
        PlanNode::Filter { .. } => "filter",
        PlanNode::Latest { .. } => "latest",
        PlanNode::AsOf { .. } => "as_of",
        PlanNode::SemiJoin { .. } => "semi_join",
        PlanNode::Join { .. } => "join",
        PlanNode::Window { .. } => "window",
        PlanNode::Aggregate { .. } => "aggregate",
        PlanNode::Exchange { .. } => "exchange",
        PlanNode::Project { .. } => "project",
    }
}

/// The node indices a predicate's semi-join keys come from.
fn predicate_nodes(p: &Predicate, out: &mut Vec<u32>) {
    match p {
        Predicate::SemiJoin {
            keys_from: KeysFrom::Node { node, .. },
            ..
        } => out.push(*node),
        Predicate::And { all } => all.iter().for_each(|q| predicate_nodes(q, out)),
        _ => {}
    }
}

/// A node's children: its inputs, then the nodes whose output keys it.
#[must_use]
pub fn children(node: &PlanNode) -> Vec<u32> {
    let keyed = |k: &Option<&KeysFrom>| match k {
        Some(KeysFrom::Node { node, .. }) => Some(*node),
        _ => None,
    };
    match node {
        PlanNode::Scan { .. } => Vec::new(),
        PlanNode::Filter { input, predicate } => {
            let mut out = vec![*input];
            predicate_nodes(predicate, &mut out);
            out
        }
        PlanNode::Latest { input, .. }
        | PlanNode::AsOf { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Exchange { input, .. }
        | PlanNode::Project { input, .. } => vec![*input],
        PlanNode::SemiJoin { input, keys, .. } => {
            std::iter::once(*input).chain(keyed(&Some(keys))).collect()
        }
        PlanNode::Window { input, keys, .. } => std::iter::once(*input)
            .chain(keyed(&keys.as_ref()))
            .collect(),
        PlanNode::Join { left, right, .. } => vec![*left, *right],
    }
}

fn check_columns(
    node: u32,
    what: &str,
    columns: &[String],
    may_be_empty: bool,
) -> ReplayResult<()> {
    if columns.is_empty() && !may_be_empty {
        return Err(invalid(Some(node), format!("{what} names no column")));
    }
    let mut seen = BTreeSet::new();
    if let Some(c) = columns.iter().find(|c| !seen.insert(c.as_str())) {
        return Err(invalid(Some(node), format!("{what} names {c:?} twice")));
    }
    if let Some(c) = columns.iter().find(|c| c.is_empty()) {
        return Err(invalid(
            Some(node),
            format!("{what} names an empty column {c:?}"),
        ));
    }
    Ok(())
}

fn check_tuples(node: u32, what: &str, width: usize, values: &[Vec<u8>]) -> ReplayResult<()> {
    for v in values {
        match encoding::tuple_elements(v) {
            Some(e) if e.len() == width => {}
            Some(e) => {
                return Err(invalid(
                    Some(node),
                    format!(
                        "{what} holds a key tuple of {} element(s) for {width} column(s)",
                        e.len()
                    ),
                ))
            }
            None => {
                return Err(invalid(
                    Some(node),
                    format!(
                        "{what} holds a key tuple ({} bytes) that is not a canonical tuple (ADR \
                         016)",
                        v.len()
                    ),
                ))
            }
        }
    }
    Ok(())
}

fn check_keys_from(node: u32, what: &str, width: usize, keys: &KeysFrom) -> ReplayResult<()> {
    match keys {
        KeysFrom::Run => Ok(()),
        KeysFrom::Values { values } => check_tuples(node, what, width, values),
        KeysFrom::Node { columns, .. } => {
            check_columns(node, &format!("{what}'s key source"), columns, false)?;
            if columns.len() != width {
                return Err(invalid(
                    Some(node),
                    format!(
                        "{what} keys {width} column(s) from {} column(s) of another node",
                        columns.len()
                    ),
                ));
            }
            Ok(())
        }
    }
}

fn check_predicate(node: u32, p: &Predicate) -> ReplayResult<()> {
    match p {
        Predicate::In { columns, values } => {
            check_columns(node, "the in predicate", columns, false)?;
            check_tuples(node, "the in predicate", columns.len(), values)
        }
        Predicate::SemiJoin { columns, keys_from } => {
            check_columns(node, "the semi-join predicate", columns, false)?;
            check_keys_from(node, "the semi-join predicate", columns.len(), keys_from)
        }
        Predicate::Between { column, lo, hi } => {
            check_columns(
                node,
                "the between predicate",
                std::slice::from_ref(column),
                false,
            )?;
            for (b, which) in [(lo, "lower"), (hi, "upper")] {
                match encoding::tuple_elements(b) {
                    Some(e) if e.len() == 1 && encoding::tuple_has_null(b) == Some(false) => {}
                    _ => {
                        return Err(invalid(
                            Some(node),
                            format!(
                                "the between predicate's {which} bound is not one non-null \
                                 canonical value (ADR 016)"
                            ),
                        ))
                    }
                }
            }
            if lo > hi {
                return Err(invalid(
                    Some(node),
                    "the between predicate's lower bound is above its upper bound; no row \
                     matches, so the filter is a mistake rather than a query",
                ));
            }
            Ok(())
        }
        Predicate::And { all } => {
            if all.is_empty() {
                return Err(invalid(Some(node), "an `and` predicate with no conjunct"));
            }
            all.iter().try_for_each(|q| check_predicate(node, q))
        }
        Predicate::LatestPerKey { .. } | Predicate::NotOlderThan { .. } => Err(invalid(
            Some(node),
            "a filter keeps rows; the newest version per key is a `latest` (or `as_of`) node, \
             which the plan prices through a temporal key",
        )),
        Predicate::Join { .. } => Err(invalid(
            Some(node),
            "a filter keeps rows of one input; a join of two inputs is a `join` node",
        )),
        Predicate::WindowPartition { .. } => Err(invalid(
            Some(node),
            "a window's partitions of interest are a `window` node with `keys`",
        )),
        Predicate::Text { .. }
        | Predicate::ExprIn { .. }
        | Predicate::ContainsAny { .. }
        | Predicate::ContainsAll { .. }
        | Predicate::Live { .. } => Err(invalid(
            Some(node),
            "relational replay prices `in`, semi-join, and `between` predicates (and their \
             `and`); text, expression, list-membership, and live predicates are priced as \
             recorded runs of their own kind (`bellwether why --mode ...`)",
        )),
    }
}

/// Checks a plan's shape without its datasets (module docs): version 1,
/// the root and every input in range, acyclic (a DAG: a node may feed
/// several), every node read by the root, and well-formed literals.
/// Returns the nodes in post order (inputs before the nodes they feed),
/// each once.
pub fn check_shape(plan: &ConsumerPlan) -> ReplayResult<Vec<u32>> {
    if plan.version > PLAN_VERSION {
        return Err(Error::UnsupportedVersion {
            what: "consumer plan",
            found: plan.version,
            supported: PLAN_VERSION,
        }
        .into());
    }
    if plan.version != PLAN_VERSION {
        return Err(invalid(
            None,
            format!("version {} is not a plan version; write 1", plan.version),
        ));
    }
    let n = plan.nodes.len();
    if n == 0 {
        return Err(invalid(None, "the plan has no nodes"));
    }
    if u32::try_from(n).is_err() {
        return Err(invalid(None, "the plan has more than 2^32 nodes"));
    }
    if plan.root as usize >= n {
        return Err(invalid(
            None,
            format!("the root {} is not a node (there are {n})", plan.root),
        ));
    }
    for (i, node) in plan.nodes.iter().enumerate() {
        let i = i as u32;
        for c in children(node) {
            if c as usize >= n {
                return Err(invalid(
                    Some(i),
                    format!("reads node {c}, and the plan has {n} nodes"),
                ));
            }
        }
        match node {
            PlanNode::Scan { dataset, columns } => {
                if dataset.is_empty() {
                    return Err(invalid(Some(i), "the scan names no dataset"));
                }
                check_columns(i, "the scan", columns, false)?;
            }
            PlanNode::Filter { predicate, .. } => check_predicate(i, predicate)?,
            PlanNode::Latest { columns, ts, .. } | PlanNode::AsOf { columns, ts, .. } => {
                check_columns(i, "the key", columns, false)?;
                if ts.is_empty() || columns.contains(ts) {
                    return Err(invalid(
                        Some(i),
                        format!("the version column {ts:?} must be a column other than the keys"),
                    ));
                }
            }
            PlanNode::SemiJoin { columns, keys, .. } => {
                check_columns(i, "the semi-join", columns, false)?;
                check_keys_from(i, "the semi-join", columns.len(), keys)?;
            }
            PlanNode::Join {
                on, left, right, ..
            } => {
                if on.is_empty() {
                    return Err(invalid(
                        Some(i),
                        "the join pairs no columns; a cross join is not priced",
                    ));
                }
                // `left == right` is a self-join of one node's output: a
                // shared node (module docs), priced once.
                let l: Vec<String> = on.iter().map(|(a, _)| a.clone()).collect();
                let r: Vec<String> = on.iter().map(|(_, b)| b.clone()).collect();
                check_columns(i, "the join's left side", &l, false)?;
                check_columns(i, "the join's right side", &r, false)?;
            }
            PlanNode::Window {
                partition_by, keys, ..
            } => {
                check_columns(i, "the window's partitions", partition_by, keys.is_none())?;
                if let Some(k) = keys {
                    check_keys_from(i, "the window", partition_by.len(), k)?;
                }
            }
            PlanNode::Aggregate {
                group_by, measures, ..
            } => {
                check_columns(i, "the aggregate's groups", group_by, true)?;
                if measures.is_empty() {
                    return Err(invalid(
                        Some(i),
                        "the aggregate computes no measure; list at least `count`",
                    ));
                }
            }
            PlanNode::Exchange { by, .. } => check_columns(i, "the exchange", by, true)?,
            PlanNode::Project { columns, .. } => {
                check_columns(i, "the projection", columns, false)?
            }
        }
    }
    // Depth-first from the root: 0 unseen, 1 on the path, 2 done.
    let mut state = vec![0u8; n];
    let mut order = Vec::with_capacity(n);
    let mut stack: Vec<(u32, usize)> = vec![(plan.root, 0)];
    state[plan.root as usize] = 1;
    while let Some((v, next)) = stack.pop() {
        let kids = children(&plan.nodes[v as usize]);
        if let Some(&c) = kids.get(next) {
            stack.push((v, next + 1));
            match state[c as usize] {
                0 => {
                    state[c as usize] = 1;
                    stack.push((c, 0));
                }
                1 => {
                    return Err(invalid(
                        Some(v),
                        format!("reads node {c}, which reads node {v}: the plan has a cycle"),
                    ))
                }
                // Done already: a node several nodes read (a DAG; module
                // docs), ordered once.
                _ => {}
            }
        } else {
            state[v as usize] = 2;
            order.push(v);
        }
    }
    if let Some(i) = state.iter().position(|s| *s == 0) {
        return Err(invalid(
            Some(i as u32),
            format!(
                "is not read by the root (node {}); remove it or feed it into the plan",
                plan.root
            ),
        ));
    }
    Ok(order)
}

/// One output column of a node: its name and where its values come from.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Col {
    name: String,
    scan: u32,
    column: String,
    ty: Option<ColumnType>,
}

/// A plan checked against its datasets' schemas ([`Replayer::check_plan`]).
#[derive(Clone, Debug)]
pub struct CheckedPlan {
    plan: ConsumerPlan,
    order: Vec<u32>,
    outputs: Vec<Vec<Col>>,
    /// Per node: its dataset for a scan.
    datasets: Vec<Option<String>>,
    /// Per scan node: the dataset columns the plan needs from it, in
    /// dataset schema order.
    projection: BTreeMap<u32, Vec<String>>,
    /// Whether a node keys by the run's keys.
    run_keyed: bool,
    /// Whether a node is an `as_of`.
    asof: bool,
    /// Per node: how many times nodes read it (0 for the root).
    consumers: Vec<u32>,
}

impl CheckedPlan {
    /// The plan.
    #[must_use]
    pub fn plan(&self) -> &ConsumerPlan {
        &self.plan
    }

    /// The datasets the plan scans, sorted, each once.
    #[must_use]
    pub fn datasets(&self) -> Vec<String> {
        let set: BTreeSet<&String> = self.datasets.iter().flatten().collect();
        set.into_iter().cloned().collect()
    }

    /// The dataset columns the plan reads from scan `node` (the projection
    /// pushed down to it).
    #[must_use]
    pub fn projection(&self, node: u32) -> Option<&[String]> {
        self.projection.get(&node).map(Vec::as_slice)
    }

    /// Whether some node keys by the run's keys (`KeysFrom::Run`).
    #[must_use]
    pub fn uses_run_keys(&self) -> bool {
        self.run_keyed
    }

    /// Whether some node is an `as_of`, which takes the run's instant.
    #[must_use]
    pub fn uses_asof(&self) -> bool {
        self.asof
    }

    /// How many times nodes read `node`: 2 or more for a shared node
    /// (module docs), 0 for the root.
    #[must_use]
    pub fn consumers(&self, node: u32) -> u32 {
        self.consumers.get(node as usize).copied().unwrap_or(0)
    }

    /// The `main` dataset columns `names` resolve to among `input`'s
    /// outputs, when every one comes from a scan of `main`.
    fn main_columns(&self, input: u32, names: &[String]) -> Option<Vec<String>> {
        let cols = self.outputs.get(input as usize)?;
        names
            .iter()
            .map(|n| {
                let c = cols.iter().find(|c| &c.name == n)?;
                (self.datasets.get(c.scan as usize)?.as_deref() == Some(MAIN_DATASET))
                    .then(|| c.column.clone())
            })
            .collect()
    }

    /// The column sets the plan probes `main` on, each with what probes
    /// it: whole keys (`in`, a semi-join, a window prefilter, a join, an
    /// aggregate's groups), a range (`between`), or versions (`latest`,
    /// `as_of`, with their version column).
    fn main_probes(&self) -> Vec<(Vec<String>, Probe)> {
        fn predicate(
            p: &Predicate,
            out: &mut Vec<(Vec<String>, Probe)>,
            at: &dyn Fn(&[String]) -> Option<Vec<String>>,
        ) {
            match p {
                Predicate::In { columns, .. } | Predicate::SemiJoin { columns, .. } => {
                    if let Some(c) = at(columns) {
                        out.push((c, Probe::Keys));
                    }
                }
                Predicate::Between { column, .. } => {
                    if let Some(c) = at(std::slice::from_ref(column)) {
                        out.push((c, Probe::Range));
                    }
                }
                Predicate::And { all } => all.iter().for_each(|q| predicate(q, out, at)),
                _ => {}
            }
        }
        let mut out = Vec::new();
        for node in &self.plan.nodes {
            match node {
                PlanNode::Filter {
                    input,
                    predicate: p,
                } => {
                    predicate(p, &mut out, &|c| self.main_columns(*input, c));
                }
                PlanNode::SemiJoin { input, columns, .. }
                | PlanNode::Window {
                    input,
                    partition_by: columns,
                    keys: Some(_),
                }
                | PlanNode::Aggregate {
                    input,
                    group_by: columns,
                    ..
                } => {
                    if let Some(c) = self.main_columns(*input, columns) {
                        out.push((c, Probe::Keys));
                    }
                }
                PlanNode::Latest { input, columns, ts } | PlanNode::AsOf { input, columns, ts } => {
                    if let (Some(c), Some(t)) = (
                        self.main_columns(*input, columns),
                        self.main_columns(*input, std::slice::from_ref(ts)),
                    ) {
                        out.push((c, Probe::Versions(t.concat())));
                    }
                }
                PlanNode::Join {
                    left, right, on, ..
                } => {
                    let l: Vec<String> = on.iter().map(|(a, _)| a.clone()).collect();
                    let r: Vec<String> = on.iter().map(|(_, b)| b.clone()).collect();
                    for c in [self.main_columns(*left, &l), self.main_columns(*right, &r)]
                        .into_iter()
                        .flatten()
                    {
                        out.push((c, Probe::Keys));
                    }
                }
                _ => {}
            }
        }
        out
    }

    /// Whether `spec`, declared on `main`, is one a node of this plan can
    /// price with: a key on the exact column set a node probes by whole
    /// keys or versions, or a range key on a `between` column.
    #[must_use]
    pub fn serves(&self, spec: &KeySpec) -> bool {
        self.main_probes()
            .iter()
            .any(|(cols, probe)| match (spec, probe) {
                (KeySpec::Range { column }, Probe::Range) => cols.as_slice() == [column.clone()],
                (
                    KeySpec::Temporal {
                        columns, ts_column, ..
                    },
                    Probe::Versions(ts),
                ) => same_set(columns, cols) && ts_column == ts,
                (
                    KeySpec::Exact { columns, .. } | KeySpec::Temporal { columns, .. },
                    Probe::Keys,
                )
                | (KeySpec::Bloom { columns, .. }, Probe::Keys) => same_set(columns, cols),
                _ => false,
            })
    }

    /// The `main` column sets the plan probes by whole keys, each once, in
    /// plan order: where an exact key would serve it (relational replay's
    /// candidates).
    #[must_use]
    pub fn main_key_columns(&self) -> Vec<Vec<String>> {
        let mut out: Vec<Vec<String>> = Vec::new();
        for (c, p) in self.main_probes() {
            if matches!(p, Probe::Keys) && !out.iter().any(|o| same_set(o, &c)) {
                out.push(c);
            }
        }
        out
    }

    /// The keys a node of this plan could price with on `main`, each once,
    /// in plan order (`why --plan`'s candidates, drawn from the columns the
    /// plan's filters, joins, windows, and aggregates name): an exact key
    /// on each column set probed by whole keys, a temporal key on each
    /// `latest`/`as_of` key set and version column, a range key on each
    /// `between` column.
    #[must_use]
    pub fn candidate_specs(&self) -> Vec<KeySpec> {
        let mut out: Vec<KeySpec> = Vec::new();
        for (columns, p) in self.main_probes() {
            let spec = match p {
                Probe::Keys => KeySpec::Exact {
                    columns,
                    counts: false,
                    bounds: None,
                },
                Probe::Versions(ts_column) => KeySpec::Temporal {
                    columns,
                    ts_column,
                    counts: false,
                    tombstone: None,
                },
                Probe::Range => KeySpec::Range {
                    column: columns.concat(),
                },
            };
            if !out.iter().any(|o| o.identity() == spec.identity()) {
                out.push(spec);
            }
        }
        out
    }

    /// The version columns of the plan's `as_of` nodes, as `(dataset,
    /// column)`, each once: the run's instant is in their unit.
    #[must_use]
    pub fn asof_columns(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for node in &self.plan.nodes {
            if let PlanNode::AsOf { input, ts, .. } = node {
                let col = self
                    .outputs
                    .get(*input as usize)
                    .and_then(|cols| cols.iter().find(|c| &c.name == ts));
                if let Some(c) = col {
                    let d = self
                        .datasets
                        .get(c.scan as usize)
                        .cloned()
                        .flatten()
                        .unwrap_or_default();
                    let pair = (d, c.column.clone());
                    if !out.contains(&pair) {
                        out.push(pair);
                    }
                }
            }
        }
        out
    }
}

/// How a plan node probes a column set.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Probe {
    Keys,
    Range,
    Versions(String),
}

fn resolve<'c>(node: u32, input: u32, cols: &'c [Col], name: &str) -> ReplayResult<&'c Col> {
    let mut hits = cols.iter().filter(|c| c.name == name);
    match (hits.next(), hits.next()) {
        (Some(c), None) => Ok(c),
        (None, _) => Err(invalid(
            Some(node),
            format!(
                "column {name:?} is not an output of node {input}, whose outputs are {}",
                cols.iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )),
        (Some(_), Some(_)) => Err(invalid(
            Some(node),
            format!(
                "column {name:?} names a column of both sides of a join below; project one side \
                 first"
            ),
        )),
    }
}

fn scalar(node: u32, col: &Col, role: &str) -> ReplayResult<ColumnType> {
    col.ty.clone().ok_or_else(|| {
        invalid(
            Some(node),
            format!(
                "{role} column {:?} is a nested column, which has no canonical encoding as a key",
                col.name
            ),
        )
    })
}

fn same_domain(node: u32, a: &Col, at: &ColumnType, b: &str, bt: &ColumnType) -> ReplayResult<()> {
    bellwether_core::join::check_same_domain(&a.name, at, b, bt)
        .map_err(|e| invalid(Some(node), e.to_string()))
}

impl<'a> Replayer<'a> {
    /// Attaches the datasets a consumer plan may scan besides
    /// [`MAIN_DATASET`], each its own replayer pinned to its own manifest.
    /// `main` is this replayer and cannot be attached again.
    pub fn with_datasets(mut self, datasets: BTreeMap<String, Replayer<'a>>) -> Result<Self> {
        if datasets.contains_key(MAIN_DATASET) {
            return Err(Error::InvalidArgument {
                field: "datasets",
                reason: format!(
                    "{MAIN_DATASET:?} is the data root's own name in a plan; bind the other \
                     datasets under other names"
                ),
            });
        }
        self.datasets = datasets;
        Ok(self)
    }

    /// Marks this replayer's costs as measured by `model` (a SQL table's
    /// per-unit aggregates, ADR 027): every plan node that reads it is
    /// `Measured`, and a replay pricing one names the model.
    #[must_use]
    pub fn with_measured(mut self, model: &str) -> Self {
        self.measured = Some(model.to_string());
        self
    }

    /// The model this replayer's costs are measured by, if any.
    #[must_use]
    pub fn measured(&self) -> Option<&str> {
        self.measured.as_deref()
    }

    /// The datasets attached besides `main`, by name.
    #[must_use]
    pub fn datasets(&self) -> &BTreeMap<String, Replayer<'a>> {
        &self.datasets
    }

    /// Lets a plan name this dataset `name` as well as [`MAIN_DATASET`]
    /// (the data root bound under its own name). A name attached as
    /// another dataset cannot also be the main one.
    pub fn with_main_alias(mut self, name: &str) -> Result<Self> {
        if self.datasets.contains_key(name) {
            return Err(Error::InvalidArgument {
                field: "datasets",
                reason: format!("{name:?} names two datasets; bind each name once"),
            });
        }
        if name != MAIN_DATASET && !self.aliases.iter().any(|a| a == name) {
            self.aliases.push(name.to_string());
        }
        Ok(self)
    }

    /// Whether `name` is this replayer's own dataset in a plan.
    #[must_use]
    pub fn is_main(&self, name: &str) -> bool {
        name == MAIN_DATASET || self.aliases.iter().any(|a| a == name)
    }

    /// The replayer of dataset `name`: this one for [`MAIN_DATASET`] (or
    /// one of its aliases).
    #[must_use]
    pub fn dataset(&self, name: &str) -> Option<&Replayer<'a>> {
        if self.is_main(name) {
            Some(self)
        } else {
            self.datasets.get(name)
        }
    }

    /// The segments of `spec` as this replayer plans with it: committed
    /// (read by range through the pinned snapshot) when the planner holds
    /// it complete, else the spec's transient index.
    pub(crate) fn segments_of(&self, spec: &KeySpec) -> Result<Arc<SpecSegments>> {
        if PlanSource::specs(self.planner.as_ref()).contains(spec) {
            if let Some(s) = self.planner.spec_segments(spec) {
                return Ok(s);
            }
            if let Some(s) = self.committed_segments(spec)? {
                return Ok(s);
            }
        }
        Ok(self.transient(spec)?.segments())
    }

    /// Checks `plan` against the datasets it scans (module docs).
    pub fn check_plan(&self, plan: &ConsumerPlan) -> ReplayResult<CheckedPlan> {
        let order = check_shape(plan)?;
        let n = plan.nodes.len();
        let mut outputs: Vec<Vec<Col>> = vec![Vec::new(); n];
        let mut datasets: Vec<Option<String>> = vec![None; n];
        let mut refs: Vec<(u32, String)> = Vec::new();
        let mut run_keyed = false;
        let mut asof = false;
        let mut consumers = vec![0u32; n];
        for &v in &order {
            for c in children(&plan.nodes[v as usize]) {
                consumers[c as usize] += 1;
            }
        }
        for &v in &order {
            let node = &plan.nodes[v as usize];
            let input_of = |i: u32| outputs[i as usize].clone();
            let out: Vec<Col> = match node {
                PlanNode::Scan { dataset, columns } => {
                    let r = self.dataset(dataset).ok_or_else(|| {
                        let mut bound: Vec<&str> = vec![MAIN_DATASET];
                        bound.extend(self.datasets.keys().map(String::as_str));
                        invalid(
                            Some(v),
                            format!(
                                "scans dataset {dataset:?}, which is not bound (bound: {}); bind \
                                 it with --dataset {dataset}=<folder>",
                                bound.join(", ")
                            ),
                        )
                    })?;
                    // An alias of the data root is `main` from here on.
                    datasets[v as usize] = Some(if self.is_main(dataset) {
                        MAIN_DATASET.to_string()
                    } else {
                        dataset.clone()
                    });
                    let schema = r.schema();
                    columns
                        .iter()
                        .map(|c| {
                            let f = schema.field_with_name(c).map_err(|_| {
                                invalid(
                                    Some(v),
                                    format!(
                                        "scans dataset {dataset}: {}",
                                        encoding::not_in_schema(schema, c)
                                    ),
                                )
                            })?;
                            Ok(Col {
                                name: c.clone(),
                                scan: v,
                                column: c.clone(),
                                ty: ColumnType::of(f.data_type()).ok(),
                            })
                        })
                        .collect::<ReplayResult<Vec<_>>>()?
                }
                PlanNode::Filter { input, predicate } => {
                    let cols = input_of(*input);
                    self.check_predicate_columns(
                        plan,
                        &outputs,
                        v,
                        *input,
                        &cols,
                        predicate,
                        &mut refs,
                        &mut run_keyed,
                    )?;
                    cols
                }
                PlanNode::Latest { input, columns, ts } | PlanNode::AsOf { input, columns, ts } => {
                    asof |= matches!(node, PlanNode::AsOf { .. });
                    let cols = input_of(*input);
                    for c in columns.iter().chain(std::iter::once(ts)) {
                        let col = resolve(v, *input, &cols, c)?;
                        scalar(v, col, "a version")?;
                        refs.push((col.scan, col.column.clone()));
                    }
                    let t = resolve(v, *input, &cols, ts)?;
                    if matches!(
                        t.ty,
                        Some(ColumnType::Float16 | ColumnType::Float32 | ColumnType::Float64)
                    ) {
                        return Err(invalid(
                            Some(v),
                            format!("the version column {ts:?} is a float; order versions by a time or integer column"),
                        ));
                    }
                    cols
                }
                PlanNode::SemiJoin {
                    input,
                    columns,
                    keys,
                } => {
                    let cols = input_of(*input);
                    self.check_keyed(
                        plan,
                        &outputs,
                        v,
                        *input,
                        &cols,
                        columns,
                        keys,
                        &mut refs,
                        &mut run_keyed,
                    )?;
                    cols
                }
                PlanNode::Window {
                    input,
                    partition_by,
                    keys,
                } => {
                    let cols = input_of(*input);
                    for c in partition_by {
                        let col = resolve(v, *input, &cols, c)?;
                        refs.push((col.scan, col.column.clone()));
                    }
                    if let Some(k) = keys {
                        self.check_keyed(
                            plan,
                            &outputs,
                            v,
                            *input,
                            &cols,
                            partition_by,
                            k,
                            &mut refs,
                            &mut run_keyed,
                        )?;
                    }
                    cols
                }
                PlanNode::Join {
                    left, right, on, ..
                } => {
                    let (l, r) = (input_of(*left), input_of(*right));
                    for (a, b) in on {
                        let (ca, cb) = (resolve(v, *left, &l, a)?, resolve(v, *right, &r, b)?);
                        let (ta, tb) = (scalar(v, ca, "a join")?, scalar(v, cb, "a join")?);
                        same_domain(v, ca, &ta, &cb.name, &tb)?;
                        refs.push((ca.scan, ca.column.clone()));
                        refs.push((cb.scan, cb.column.clone()));
                    }
                    // A pair that names one column on both sides is one
                    // column above the join (`using`): the left's.
                    let mut out = l;
                    for c in r {
                        if !on.iter().any(|(a, b)| a == b && *b == c.name) {
                            out.push(c);
                        }
                    }
                    out
                }
                PlanNode::Aggregate {
                    input,
                    group_by,
                    measures,
                } => {
                    let cols = input_of(*input);
                    let mut out = Vec::with_capacity(group_by.len());
                    for c in group_by {
                        let col = resolve(v, *input, &cols, c)?;
                        refs.push((col.scan, col.column.clone()));
                        out.push(col.clone());
                    }
                    for m in measures {
                        if let Some(c) = &m.column {
                            let col = resolve(v, *input, &cols, c)?;
                            refs.push((col.scan, col.column.clone()));
                        } else if m.function != bellwether_types::AggregateFn::Count {
                            return Err(invalid(
                                Some(v),
                                format!(
                                    "{} needs a column; only count takes none",
                                    m.function.as_str()
                                ),
                            ));
                        }
                    }
                    out
                }
                PlanNode::Exchange { input, by } => {
                    let cols = input_of(*input);
                    for c in by {
                        let col = resolve(v, *input, &cols, c)?;
                        refs.push((col.scan, col.column.clone()));
                    }
                    cols
                }
                PlanNode::Project { input, columns } => {
                    let cols = input_of(*input);
                    columns
                        .iter()
                        .map(|c| resolve(v, *input, &cols, c).cloned())
                        .collect::<ReplayResult<Vec<_>>>()?
                }
            };
            outputs[v as usize] = out;
        }
        for c in &outputs[plan.root as usize] {
            refs.push((c.scan, c.column.clone()));
        }
        let mut projection: BTreeMap<u32, Vec<String>> = BTreeMap::new();
        for (v, d) in datasets.iter().enumerate() {
            let Some(d) = d else { continue };
            let schema = self.dataset(d).map(|r| Arc::clone(r.schema()));
            let wanted: BTreeSet<&str> = refs
                .iter()
                .filter(|(s, _)| *s as usize == v)
                .map(|(_, c)| c.as_str())
                .collect();
            let ordered = schema.map_or_else(Vec::new, |s| {
                s.fields()
                    .iter()
                    .filter(|f| wanted.contains(f.name().as_str()))
                    .map(|f| f.name().clone())
                    .collect()
            });
            projection.insert(v as u32, ordered);
        }
        Ok(CheckedPlan {
            plan: plan.clone(),
            order,
            outputs,
            datasets,
            projection,
            run_keyed,
            asof,
            consumers,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn check_predicate_columns(
        &self,
        plan: &ConsumerPlan,
        outputs: &[Vec<Col>],
        v: u32,
        input: u32,
        cols: &[Col],
        p: &Predicate,
        refs: &mut Vec<(u32, String)>,
        run_keyed: &mut bool,
    ) -> ReplayResult<()> {
        match p {
            Predicate::In { columns, .. } => {
                for c in columns {
                    let col = resolve(v, input, cols, c)?;
                    scalar(v, col, "a key")?;
                    refs.push((col.scan, col.column.clone()));
                }
                Ok(())
            }
            Predicate::SemiJoin { columns, keys_from } => self.check_keyed(
                plan, outputs, v, input, cols, columns, keys_from, refs, run_keyed,
            ),
            Predicate::Between { column, .. } => {
                let col = resolve(v, input, cols, column)?;
                let t = scalar(v, col, "a range")?;
                if matches!(
                    t,
                    ColumnType::Float16 | ColumnType::Float32 | ColumnType::Float64
                ) {
                    return Err(invalid(
                        Some(v),
                        format!(
                            "the between predicate is on the float column {column:?}: -0.0 and \
                             0.0 encode apart and NaN orders differently across engines, so a \
                             priced range would guess; range over a decimal, integer, or time \
                             column"
                        ),
                    ));
                }
                refs.push((col.scan, col.column.clone()));
                Ok(())
            }
            Predicate::And { all } => all.iter().try_for_each(|q| {
                self.check_predicate_columns(plan, outputs, v, input, cols, q, refs, run_keyed)
            }),
            // Refused by `check_shape`.
            _ => Err(invalid(Some(v), "this predicate is not priced")),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn check_keyed(
        &self,
        plan: &ConsumerPlan,
        outputs: &[Vec<Col>],
        v: u32,
        input: u32,
        cols: &[Col],
        columns: &[String],
        keys: &KeysFrom,
        refs: &mut Vec<(u32, String)>,
        run_keyed: &mut bool,
    ) -> ReplayResult<()> {
        let mut resolved = Vec::with_capacity(columns.len());
        for c in columns {
            let col = resolve(v, input, cols, c)?;
            let t = scalar(v, col, "a key")?;
            refs.push((col.scan, col.column.clone()));
            resolved.push((col.clone(), t));
        }
        match keys {
            KeysFrom::Run => *run_keyed = true,
            KeysFrom::Values { .. } => {}
            KeysFrom::Node {
                node,
                columns: from,
            } => {
                let source = &outputs[*node as usize];
                debug_assert!(plan.nodes.len() > *node as usize);
                for ((col, t), f) in resolved.iter().zip(from) {
                    let fc = resolve(v, *node, source, f)?;
                    let ft = scalar(v, fc, "a key")?;
                    same_domain(v, col, t, &fc.name, &ft)?;
                    refs.push((fc.scan, fc.column.clone()));
                }
            }
        }
        Ok(())
    }

    /// Prices `plan` for one run (module docs): `main` under `specs`, every
    /// other dataset under its current specs; `keys` feed the plan's
    /// `KeysFrom::Run` nodes and `asof` its `as_of` nodes. Returns the
    /// run's cost, each node's cost in plan order, and whether a key
    /// selection no spec served was priced under
    /// [`crate::Baseline::ParquetStats`].
    pub fn price_plan(
        &self,
        plan: &ConsumerPlan,
        specs: &[KeySpec],
        run_id: &str,
        keys: &RunKeys,
        asof: Option<i64>,
    ) -> ReplayResult<PricedPlan> {
        let checked = self.check_plan(plan)?;
        self.price_checked(&checked, specs, run_id, keys, asof)
    }

    /// [`Replayer::price_plan`] for a plan already checked.
    pub fn price_checked(
        &self,
        checked: &CheckedPlan,
        specs: &[KeySpec],
        run_id: &str,
        keys: &RunKeys,
        asof: Option<i64>,
    ) -> ReplayResult<PricedPlan> {
        if checked.asof && asof.is_none() {
            return Err(invalid(
                None,
                format!(
                    "the plan has an as_of node and run {run_id:?} records no instant; record the \
                     run as an asof run (`--at`)"
                ),
            ));
        }
        let mut planners: BTreeMap<String, Box<dyn PlanSource + '_>> = BTreeMap::new();
        let mut configs: BTreeMap<String, Vec<KeySpec>> = BTreeMap::new();
        for d in checked.datasets() {
            let r = self
                .dataset(&d)
                .ok_or_else(|| invalid(None, format!("{d} is not bound")))?;
            let s = if d == MAIN_DATASET {
                specs.to_vec()
            } else {
                r.current_specs().to_vec()
            };
            planners.insert(d.clone(), r.planner_for(&s)?);
            configs.insert(d, s);
        }
        let mut p = Pricer {
            main: self,
            checked,
            planners,
            configs,
            run_keys: keys,
            asof,
            unserved: false,
            remaining: checked.consumers.clone(),
        };
        p.price(run_id)
    }

    /// The catalog digests of the datasets `checked` scans besides `main`,
    /// by dataset name.
    #[must_use]
    pub fn plan_inputs(&self, checked: &CheckedPlan) -> Vec<Hash> {
        checked
            .datasets()
            .iter()
            .filter(|d| d.as_str() != MAIN_DATASET)
            .filter_map(|d| self.datasets.get(d))
            .map(|r| r.catalog().digest())
            .collect()
    }
}

/// One run priced through a plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PricedPlan {
    /// The run's cost: the root's read bytes, every read occurrence's row
    /// groups and files, and the root's rows.
    pub cost: RunCost,
    /// Each node's cost, in plan order.
    pub operators: Vec<OperatorCost>,
    /// Every read occurrence the root's cost sums (one per `scan` node,
    /// module docs), in node order.
    pub reads: Vec<ScanRead>,
    /// Every node more than one node reads, in node order (module docs).
    pub shared: Vec<SharedOutput>,
    /// The model a measured dataset's costs come from, when one was read.
    pub model: Option<String>,
    /// Whether a key selection no spec served was priced under the
    /// statistics baseline.
    pub unserved: bool,
}

/// One read occurrence of a run: a `scan` node, read once with the union
/// of the row groups every consumer path plans from it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRead {
    /// The scan node.
    pub node: u32,
    /// Its dataset (`main` for the data root and its aliases).
    pub dataset: String,
    /// The live row-group ids it reads, ascending.
    pub row_groups: Vec<u32>,
    /// The projected compressed bytes of those row groups.
    pub bytes: u64,
}

/// A node several nodes read: evaluated once (logical reuse, module docs),
/// its output read by each consumer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SharedOutput {
    /// The node.
    pub node: u32,
    /// How many times nodes read it (a self-join reads it twice).
    pub consumers: u32,
    /// A compressed-equivalent **model estimate** of its output, `[low,
    /// high]` bytes: `rows_out` (its interval) × the average compressed
    /// row width of the planned row groups of every read below it. A
    /// model, not a statistic: it bounds neither runtime memory nor RSS,
    /// which stay unknown until measured.
    pub estimate: (u64, u64),
}

/// An interval `[lo, hi]` that holds the truth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Iv {
    lo: u64,
    hi: u64,
}

impl Iv {
    fn exact(n: u64) -> Self {
        Self { lo: n, hi: n }
    }
    fn new(lo: u64, hi: u64) -> Self {
        debug_assert!(lo <= hi, "an empty interval [{lo}, {hi}]");
        Self { lo: lo.min(hi), hi }
    }
}

fn sat(x: u128) -> u64 {
    u64::try_from(x).unwrap_or(u64::MAX)
}

/// What is proved about which rows of a scan a side holds.
#[derive(Clone, Debug)]
enum Sel {
    /// Every row of the dataset.
    All,
    /// Exactly the rows whose tuple on the key set's columns is one of its
    /// keys.
    Keys(RunKeys),
    /// A subset, by a filter no index proves.
    Other,
}

/// The rows of one scan as narrowed so far.
#[derive(Clone, Debug)]
struct Side {
    scan: u32,
    dataset: String,
    rgs: RoaringBitmap,
    rows: Iv,
    sel: Sel,
    /// Column sets a node below computed over whole groups of (a `latest`
    /// or `as_of` over its keys, a `window` over its partitions): a node
    /// above may drop row groups from the read only by whole keys on
    /// exactly each of these, or it would cut a group short (a stale
    /// version promoted, a window over part of its partition).
    groups: Vec<Vec<String>>,
}

impl Side {
    /// Whether a selection on `columns` (whole keys; `None` for a range or
    /// any other) may drop row groups from this side's read.
    fn narrowable(&self, columns: Option<&[String]>) -> bool {
        self.groups
            .iter()
            .all(|g| columns.is_some_and(|c| same_set(g, c)))
    }

    /// `planned` as this side's read: kept when narrowable by `columns`,
    /// else the read stays the input's (rows are still priced from
    /// `planned`).
    fn read(&self, columns: Option<&[String]>, planned: RoaringBitmap) -> RoaringBitmap {
        if self.narrowable(columns) {
            planned
        } else {
            self.rgs.clone()
        }
    }
}

/// A node's output: one scan's rows (which an index can still narrow), or
/// rows derived from several (a join's, an aggregate's), with every side
/// read to produce it.
#[derive(Clone, Debug)]
struct Rel {
    side: Option<Side>,
    reads: Vec<Side>,
    rows: Iv,
}

impl Rel {
    fn of(side: Side) -> Self {
        Self {
            rows: side.rows,
            side: Some(side),
            reads: Vec::new(),
        }
    }
    fn with_side(mut self, side: Side) -> Self {
        self.rows = side.rows;
        self.side = Some(side);
        self
    }
    fn sides(&self) -> impl Iterator<Item = &Side> {
        self.side.iter().chain(&self.reads)
    }
    fn into_reads(self) -> Vec<Side> {
        self.side.into_iter().chain(self.reads).collect()
    }
    /// Every read occurrence below: per scan node its dataset and the
    /// union of the row groups every path to it reads (module docs).
    fn occurrences(&self) -> BTreeMap<u32, (&str, RoaringBitmap)> {
        let mut out: BTreeMap<u32, (&str, RoaringBitmap)> = BTreeMap::new();
        for s in self.sides() {
            out.entry(s.scan)
                .or_insert_with(|| (s.dataset.as_str(), RoaringBitmap::new()))
                .1 |= &s.rgs;
        }
        out
    }
}

/// What a merge of two sides' keys on their join columns found.
#[derive(Default)]
struct Merged {
    common: u64,
    lrgs: RoaringBitmap,
    rrgs: RoaringBitmap,
    lsum: Option<u64>,
    rsum: Option<u64>,
    prod: Option<u128>,
    /// Whether each side is proved to hold exactly every row of the keys
    /// it holds (a scan's rows selected only by whole keys on its join
    /// columns), so the common keys are the relations' own.
    lpure: bool,
    rpure: bool,
}

struct Pricer<'p, 'a> {
    main: &'p Replayer<'a>,
    checked: &'p CheckedPlan,
    planners: BTreeMap<String, Box<dyn PlanSource + 'p>>,
    configs: BTreeMap<String, Vec<KeySpec>>,
    run_keys: &'p RunKeys,
    asof: Option<i64>,
    unserved: bool,
    /// Per node: the reads of it not yet priced; its output is moved to
    /// the last, cloned for the others (a shared node, module docs).
    remaining: Vec<u32>,
}

fn same_set(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().all(|c| b.contains(c))
}

fn keyed_columns(spec: &KeySpec) -> Option<&[String]> {
    match spec {
        KeySpec::Exact { columns, .. } | KeySpec::Temporal { columns, .. } => Some(columns),
        _ => None,
    }
}

impl<'p, 'a> Pricer<'p, 'a> {
    fn replayer(&self, d: &str) -> ReplayResult<&'p Replayer<'a>> {
        self.main
            .dataset(d)
            .ok_or_else(|| invalid(None, format!("dataset {d} is not bound")))
    }

    fn specs(&self, d: &str) -> &[KeySpec] {
        self.configs.get(d).map_or(&[], Vec::as_slice)
    }

    fn planner(&self, d: &str) -> ReplayResult<&(dyn PlanSource + 'p)> {
        self.planners
            .get(d)
            .map(AsRef::as_ref)
            .ok_or_else(|| invalid(None, format!("dataset {d} has no planner")))
    }

    fn column_of(&self, node: u32, input: u32, name: &str) -> ReplayResult<Col> {
        resolve(node, input, &self.checked.outputs[input as usize], name).cloned()
    }

    /// Rows of `rgs` in dataset `d` (footer sums).
    fn rows_of(&self, d: &str, rgs: &RoaringBitmap) -> ReplayResult<u64> {
        let c = self.replayer(d)?.catalog();
        let mut n = 0u64;
        for id in rgs {
            n = n.saturating_add(c.get(id).map_or(0, |r| r.meta.rows));
        }
        Ok(n)
    }

    /// The projected compressed bytes of read occurrence `scan` (of
    /// dataset `d`) over row groups `rgs`.
    fn read_bytes(&self, scan: u32, d: &str, rgs: &RoaringBitmap) -> ReplayResult<u64> {
        let r = self.replayer(d)?;
        let c = r.catalog();
        let projection = self.checked.projection(scan).unwrap_or(&[]);
        let mut total = 0u64;
        for id in rgs {
            let rg = c
                .get(id)
                .ok_or_else(|| Error::Other(format!("row group {id} is not live in {d}")))?;
            // A text chunk is read whole, whatever columns are wanted.
            if rg.meta.byte_range.is_some() {
                total = total.saturating_add(rg.meta.bytes);
                continue;
            }
            for col in projection {
                let b = rg
                    .meta
                    .stats
                    .iter()
                    .find(|s| &s.column == col)
                    .and_then(|s| s.bytes);
                match b {
                    Some(b) => total = total.saturating_add(b),
                    None if r.measured.is_some() => {
                        total = total.saturating_add(rg.meta.bytes);
                        break;
                    }
                    None => {
                        return Err(Error::Source(format!(
                            "{}: row group {} reports no compressed size for column {col:?}, so \
                             its projection cannot be priced",
                            rg.meta.file.path, rg.meta.index_in_file
                        ))
                        .into())
                    }
                }
            }
        }
        Ok(total)
    }

    fn read_files(&self, d: &str, rgs: &RoaringBitmap) -> ReplayResult<u64> {
        let c = self.replayer(d)?.catalog();
        let files: BTreeSet<usize> = rgs
            .iter()
            .filter_map(|id| c.get(id))
            .map(|r| r.file)
            .collect();
        Ok(files.len() as u64)
    }

    /// The reads below `rel`: each read occurrence once (module docs).
    fn rel_bytes(&self, rel: &Rel) -> ReplayResult<u64> {
        let mut t = 0u64;
        for (scan, (d, rgs)) in rel.occurrences() {
            t = t.saturating_add(self.read_bytes(scan, d, &rgs)?);
        }
        Ok(t)
    }

    fn model_of(&self, rel: &Rel) -> Option<String> {
        rel.sides()
            .filter_map(|s| {
                self.main
                    .dataset(&s.dataset)
                    .and_then(|r| r.measured.clone())
            })
            .next()
    }

    fn quantity(iv: Iv, model: Option<&String>) -> Quantity {
        match model {
            Some(m) => Quantity {
                value: iv.hi,
                exactness: Exactness::Measured { model: m.clone() },
            },
            None if iv.lo == iv.hi => Quantity::exact(iv.hi),
            None => Quantity {
                value: iv.hi,
                exactness: Exactness::Bounded {
                    low: iv.lo,
                    high: iv.hi,
                },
            },
        }
    }

    /// Keys for `columns` of `side`'s dataset from `from`: the run's keys
    /// relabelled, literal tuples, or (`None`) another node's output.
    fn keys_for(
        &self,
        node: u32,
        side: &Side,
        columns: &[String],
        from: &KeysFrom,
    ) -> ReplayResult<Option<RunKeys>> {
        let schema = self.replayer(&side.dataset)?.schema();
        let types = columns
            .iter()
            .map(|c| {
                let f = schema.field_with_name(c).map_err(|_| {
                    invalid(Some(node), format!("{c:?} is not in {}", side.dataset))
                })?;
                ColumnType::of(f.data_type()).map_err(ReplayError::from)
            })
            .collect::<ReplayResult<Vec<_>>>()?;
        match from {
            KeysFrom::Run => {
                let rk = self.run_keys;
                if rk.columns().len() != columns.len() {
                    return Err(invalid(
                        Some(node),
                        format!(
                            "keys by the run's keys on {} column(s) ({}), and the run looks up {} \
                             ({}); record the run's keys on as many columns as the node keys",
                            columns.len(),
                            columns.join(", "),
                            rk.columns().len(),
                            rk.columns().join(", ")
                        ),
                    ));
                }
                for ((rc, rt), (c, t)) in rk
                    .columns()
                    .iter()
                    .zip(rk.key_types())
                    .zip(columns.iter().zip(&types))
                {
                    bellwether_core::join::check_same_domain(rc, rt, c, t)
                        .map_err(|e| invalid(Some(node), e.to_string()))?;
                }
                Ok(Some(rk.relabel(columns, &types)?))
            }
            KeysFrom::Values { values } => Ok(Some(
                RunKeys::from_tuples(columns, &types, values.iter().map(Vec::as_slice))?.0,
            )),
            KeysFrom::Node { .. } => Ok(None),
        }
    }

    /// A keyed (exact or temporal) spec of `d`'s configuration on exactly
    /// the columns `columns` (in any order), preferring one with counts.
    fn keyed_spec(&self, d: &str, columns: &[String], counts: bool) -> Option<KeySpec> {
        let specs = self.specs(d);
        let fit = |s: &&KeySpec| keyed_columns(s).is_some_and(|c| same_set(c, columns));
        if counts {
            specs.iter().filter(fit).find(|s| s.counts()).cloned()
        } else {
            specs
                .iter()
                .filter(fit)
                .find(|s| s.counts())
                .or_else(|| specs.iter().find(fit))
                .cloned()
        }
    }

    /// Every stored key of `keys` (projected onto `spec`'s columns) with its
    /// row groups and per-row-group counts, folded across segments.
    fn stored(
        &self,
        d: &str,
        spec: &KeySpec,
        keys: &RunKeys,
    ) -> ReplayResult<(Vec<usize>, BTreeMap<Vec<u8>, Folded>)> {
        let cols = keyed_columns(spec).unwrap_or(&[]);
        let positions = keys
            .positions(cols)
            .ok_or_else(|| invalid(None, "a key spec does not cover the keys"))?;
        let probes = keys.all_probes(&positions);
        let segs = self.replayer(d)?.segments_of(spec)?;
        let mut out: BTreeMap<Vec<u8>, Folded> = BTreeMap::new();
        for r in segs.readers() {
            for (k, v) in r.get(&probes)? {
                out.entry(k).or_default().add(&v);
            }
        }
        Ok((positions, out))
    }

    /// `(key, row group)` pairs of `keys` in `rgs`: each is at least one
    /// row of a distinct stored key, so their number bounds the rows from
    /// below. 0 without a keyed spec on exactly the keys' columns.
    fn pairs(&self, d: &str, keys: &RunKeys, rgs: &RoaringBitmap) -> ReplayResult<u64> {
        let Some(spec) = self.keyed_spec(d, keys.columns(), false) else {
            return Ok(0);
        };
        let (_, stored) = self.stored(d, &spec, keys)?;
        Ok(stored.values().map(|f| (&f.bitmap & rgs).len()).sum())
    }

    /// Counted rows of `keys` in `rgs`, from a counted keyed spec on
    /// exactly the keys' columns; `None` without one.
    fn counted(&self, d: &str, keys: &RunKeys, rgs: &RoaringBitmap) -> ReplayResult<Option<u64>> {
        let Some(spec) = self.keyed_spec(d, keys.columns(), true) else {
            return Ok(None);
        };
        let (_, stored) = self.stored(d, &spec, keys)?;
        let mut n = 0u64;
        for f in stored.values() {
            n = n.saturating_add(f.rows_in(rgs).unwrap_or(0));
        }
        Ok(Some(n))
    }

    /// `side` filtered to the rows whose `columns` tuple is one of `keys`.
    fn select_keys(&mut self, side: Side, keys: RunKeys) -> ReplayResult<Side> {
        let d = side.dataset.clone();
        let (keys, base) = match &side.sel {
            Sel::Keys(prev) if prev.columns() == keys.columns() => (prev.intersect(&keys)?, true),
            Sel::All => (keys, true),
            _ => (keys, false),
        };
        let r = self.replayer(&d)?;
        let (plan, unserved) = {
            let planner = self.planner(&d)?;
            r.verified_plan(planner, self.specs(&d), &keys, &RunQuery::Locate)?
        };
        self.unserved |= unserved;
        let planned = &side.rgs & &r.catalog().ids_of(&plan);
        let in_rgs = self.rows_of(&d, &planned)?;
        let lower = if base {
            self.pairs(&d, &keys, &planned)?
        } else {
            0
        };
        let rgs = side.read(Some(keys.columns()), planned);
        if base {
            // The relation is exactly the dataset's rows of `keys`.
            let rows = match &plan.rows {
                Some(q) if q.exactness.is_exact() => Iv::exact(q.value),
                _ => Iv::new(lower, in_rgs),
            };
            return Ok(Side {
                rows,
                rgs,
                sel: Sel::Keys(keys),
                ..side
            });
        }
        let mut hi = side.rows.hi.min(in_rgs);
        if let Some(q) = &plan.rows {
            hi = hi.min(q.value);
        }
        Ok(Side {
            rows: Iv::new(0, hi),
            rgs,
            sel: Sel::Other,
            ..side
        })
    }

    /// `side` filtered to `column` in `[lo, hi]`.
    fn select_range(
        &mut self,
        side: Side,
        column: &str,
        lo: &[u8],
        hi: &[u8],
    ) -> ReplayResult<Side> {
        let d = side.dataset.clone();
        let range = self
            .specs(&d)
            .iter()
            .find(|s| matches!(s, KeySpec::Range { column: c } if c == column))
            .cloned();
        let planned: RoaringBitmap = if let Some(spec) = range {
            let segs = self.replayer(&d)?.segments_of(&spec)?;
            let mut planned = RoaringBitmap::new();
            for r in segs.readers() {
                planned |= r.lookup_range(lo, hi)?;
            }
            &side.rgs & &planned
        } else {
            let c = self.replayer(&d)?.catalog();
            side.rgs
                .iter()
                .filter(|id| {
                    c.get(*id).is_some_and(|rg| {
                        rg.meta
                            .stats
                            .iter()
                            .find(|s| s.column == column)
                            .is_none_or(|s| {
                                !(s.max.as_deref().is_some_and(|m| m < lo)
                                    || s.min.as_deref().is_some_and(|m| m > hi))
                            })
                    })
                })
                .collect()
        };
        // Rows in range lie in the planned row groups, read or not.
        let hi_rows = side.rows.hi.min(self.rows_of(&d, &planned)?);
        let rgs = side.read(None, planned);
        Ok(Side {
            rows: Iv::new(0, hi_rows),
            rgs,
            sel: Sel::Other,
            ..side
        })
    }

    /// `side` reduced to each key's newest version on `ts` (at or before
    /// the run's instant for `as_of`).
    fn select_versions(
        &mut self,
        side: Side,
        columns: &[String],
        ts: &str,
        asof: Option<i64>,
    ) -> ReplayResult<Side> {
        let d = side.dataset.clone();
        let keys = match &side.sel {
            Sel::Keys(k) if same_set(k.columns(), columns) => Some(k.clone()),
            _ => None,
        };
        let temporal = self
            .specs(&d)
            .iter()
            .find(|s| {
                matches!(s, KeySpec::Temporal { columns: c, ts_column, .. }
                    if same_set(c, columns) && ts_column == ts)
            })
            .cloned();
        let mut groups = side.groups.clone();
        groups.push(columns.to_vec());
        let (Some(keys), Some(spec)) = (keys, temporal) else {
            // No temporal key proves the winners of exactly these rows:
            // the node reads its input, and a node above may narrow that
            // read only by whole keys on these columns.
            return Ok(Side {
                rows: Iv::new(0, side.rows.hi),
                sel: Sel::Other,
                groups,
                ..side
            });
        };
        let r = self.replayer(&d)?;
        let query = match asof {
            Some(at) => RunQuery::Asof { ts: at },
            None => RunQuery::Latest,
        };
        let (plan, unserved) = {
            let planner = self.planner(&d)?;
            r.verified_plan(planner, self.specs(&d), &keys, &query)?
        };
        self.unserved |= unserved;
        let planned = &side.rgs & &r.catalog().ids_of(&plan);
        // Lower: a key with a live version (at or before the instant)
        // returns at least one row; one per run tuple, so the two
        // encodings of a float zero count once.
        let live = r.catalog().all_ids();
        let (positions, stored) = self.stored(&d, &spec, &keys)?;
        let mut lower = 0u64;
        for i in 0..keys.len() {
            let has = keys.probes(i, &positions).iter().any(|p| {
                stored.get(p).is_some_and(|f| {
                    f.versions
                        .iter()
                        .any(|(t, rg)| live.contains(*rg) && asof.is_none_or(|a| *t <= a))
                })
            });
            lower += u64::from(has);
        }
        let mut upper = side.rows.hi.min(self.rows_of(&d, &planned)?);
        if let Some(n) = self.counted(&d, &keys, &planned)? {
            upper = upper.min(n);
        }
        Ok(Side {
            rows: Iv::new(lower, upper),
            rgs: side.read(Some(columns), planned),
            sel: Sel::Other,
            groups,
            ..side
        })
    }

    /// Whether `side` holds exactly every row, in the dataset, of each key
    /// it holds on `columns`, and its whole-key selection on them if any.
    fn proved(side: &Side, columns: &[String]) -> (bool, Option<RunKeys>) {
        match &side.sel {
            Sel::All => (true, None),
            Sel::Keys(k) if same_set(k.columns(), columns) => (true, Some(k.clone())),
            _ => (false, None),
        }
    }

    /// The keys two sides share on their join columns (`lcols[i]` pairs
    /// with `rcols[i]`), by a merge of both sides' exact or temporal keys.
    fn merge(
        &self,
        node: u32,
        (lname, left, lcols): (&'static str, &Side, &[String]),
        (rname, right, rcols): (&'static str, &Side, &[String]),
    ) -> ReplayResult<Merged> {
        let not_indexed =
            |side: &'static str, s: &Side, cols: &[String]| ReplayError::JoinNotIndexed {
                node,
                side,
                dataset: s.dataset.clone(),
                columns: cols.to_vec(),
                derived: None,
            };
        let lspecs: Vec<&KeySpec> = self
            .specs(&left.dataset)
            .iter()
            .filter(|s| keyed_columns(s).is_some_and(|c| same_set(c, lcols)))
            .collect();
        if lspecs.is_empty() {
            return Err(not_indexed(lname, left, lcols));
        }
        let pair = |l: &str| -> String {
            lcols
                .iter()
                .position(|c| c == l)
                .map(|i| rcols[i].clone())
                .unwrap_or_default()
        };
        let mut found: Option<(KeySpec, KeySpec)> = None;
        // The right key the first left key would merge with: the one to
        // declare when none does (its columns in that key's order).
        let mut wanted: Option<Vec<String>> = None;
        for ls in &lspecs {
            let lorder = keyed_columns(ls).unwrap_or(&[]);
            let want: Vec<String> = lorder.iter().map(|c| pair(c)).collect();
            let rs = self
                .specs(&right.dataset)
                .iter()
                .filter(|s| keyed_columns(s) == Some(want.as_slice()));
            // Prefer counted keys on both sides.
            for r in rs {
                let better = found.as_ref().is_none_or(|(fl, fr)| {
                    u8::from(ls.counts()) + u8::from(r.counts())
                        > u8::from(fl.counts()) + u8::from(fr.counts())
                });
                if better {
                    found = Some(((*ls).clone(), r.clone()));
                }
            }
            wanted.get_or_insert(want);
        }
        let Some((lspec, rspec)) = found else {
            return Err(not_indexed(
                rname,
                right,
                wanted.as_deref().unwrap_or(rcols),
            ));
        };
        let (lpure, lkeys) = Self::proved(left, lcols);
        let (rpure, rkeys) = Self::proved(right, rcols);
        let lorder = keyed_columns(&lspec).unwrap_or(&[]).to_vec();
        let rorder = keyed_columns(&rspec).unwrap_or(&[]).to_vec();
        let lsegs = self.replayer(&left.dataset)?.segments_of(&lspec)?;
        let rsegs = self.replayer(&right.dataset)?.segments_of(&rspec)?;
        let lcounted = lspec.counts();
        let rcounted = rspec.counts();
        let mut m = Merged {
            lsum: lcounted.then_some(0),
            rsum: rcounted.then_some(0),
            prod: (lcounted && rcounted).then_some(0),
            lpure,
            rpure,
            ..Merged::default()
        };
        let mut visit = |k: &[u8], lf: &Folded, rf: &Folded| {
            if encoding::tuple_has_null(k) != Some(false) {
                return;
            }
            let lb = &lf.bitmap & &left.rgs;
            let rb = &rf.bitmap & &right.rgs;
            if lb.is_empty() || rb.is_empty() {
                return;
            }
            m.common += 1;
            let cl = lf.rows_in(&lb);
            let cr = rf.rows_in(&rb);
            m.lrgs |= lb;
            m.rrgs |= rb;
            if let (Some(s), Some(c)) = (m.lsum.as_mut(), cl) {
                *s = s.saturating_add(c);
            }
            if let (Some(s), Some(c)) = (m.rsum.as_mut(), cr) {
                *s = s.saturating_add(c);
            }
            if let (Some(p), Some(a), Some(b)) = (m.prod.as_mut(), cl, cr) {
                *p = p.saturating_add(u128::from(a) * u128::from(b));
            }
        };
        // A side selected by whole keys on its join columns restricts the
        // merge to those keys (the run's keys of a semi-join below): probe
        // them instead of streaming every key of both indexes.
        let restrict: Option<Vec<Vec<u8>>> = match (&lkeys, &rkeys) {
            (Some(l), Some(r)) => {
                let lp = l.all_probes(&l.positions(&lorder).unwrap_or_default());
                let rp: BTreeSet<Vec<u8>> = r
                    .all_probes(&r.positions(&rorder).unwrap_or_default())
                    .into_iter()
                    .collect();
                Some(lp.into_iter().filter(|p| rp.contains(p)).collect())
            }
            (Some(l), None) => Some(l.all_probes(&l.positions(&lorder).unwrap_or_default())),
            (None, Some(r)) => Some(r.all_probes(&r.positions(&rorder).unwrap_or_default())),
            (None, None) => None,
        };
        if let Some(probes) = restrict {
            let mut lmap: BTreeMap<Vec<u8>, Folded> = BTreeMap::new();
            let mut rmap: BTreeMap<Vec<u8>, Folded> = BTreeMap::new();
            for r in lsegs.readers() {
                for (k, v) in r.get(&probes)? {
                    lmap.entry(k).or_default().add(&v);
                }
            }
            for r in rsegs.readers() {
                for (k, v) in r.get(&probes)? {
                    rmap.entry(k).or_default().add(&v);
                }
            }
            for (k, lf) in &lmap {
                if let Some(rf) = rmap.get(k) {
                    visit(k, lf, rf);
                }
            }
        } else {
            let mut ls = MergedKeys::new(lsegs.readers())?;
            let mut rs = MergedKeys::new(rsegs.readers())?;
            let (mut l, mut r) = (ls.next_key()?, rs.next_key()?);
            while let (Some((lk, lv)), Some((rk, rv))) = (&l, &r) {
                match lk.cmp(rk) {
                    std::cmp::Ordering::Less => l = ls.next_key()?,
                    std::cmp::Ordering::Greater => r = rs.next_key()?,
                    std::cmp::Ordering::Equal => {
                        visit(lk, &Folded::of(lv), &Folded::of(rv));
                        l = ls.next_key()?;
                        r = rs.next_key()?;
                    }
                }
            }
        }
        Ok(m)
    }

    /// Node `i`'s output as node `v` reads it: moved to its last reader,
    /// cloned for the others (a shared node is priced once; module docs).
    fn input(&mut self, rels: &mut [Option<Rel>], v: u32, i: u32) -> ReplayResult<Rel> {
        let left = self
            .remaining
            .get_mut(i as usize)
            .ok_or_else(|| invalid(Some(v), format!("reads node {i}, which is not a node")))?;
        *left = left.saturating_sub(1);
        let slot = &mut rels[i as usize];
        let rel = if *left == 0 {
            slot.take()
        } else {
            slot.clone()
        };
        rel.ok_or_else(|| invalid(Some(v), format!("node {i} was not priced before it")))
    }

    fn price(&mut self, run_id: &str) -> ReplayResult<PricedPlan> {
        let plan = &self.checked.plan;
        let n = plan.nodes.len();
        let mut rels: Vec<Option<Rel>> = vec![None; n];
        let mut ops: Vec<Option<OperatorCost>> = vec![None; n];
        let mut shared = Vec::new();
        let order = self.checked.order.clone();
        for v in order {
            let node = &plan.nodes[v as usize];
            let mut moved: Option<Iv> = None;
            let rel = match node {
                PlanNode::Scan { dataset, .. } => {
                    let r = self.replayer(dataset)?;
                    let rgs = r.catalog().all_ids();
                    Rel::of(Side {
                        scan: v,
                        dataset: dataset.clone(),
                        rows: Iv::exact(r.catalog().rows_total()),
                        rgs,
                        sel: Sel::All,
                        groups: Vec::new(),
                    })
                }
                PlanNode::Filter { input, predicate } => {
                    let rel = self.input(&mut rels, v, *input)?;
                    self.filter(v, *input, rel, predicate, &mut rels)?
                }
                PlanNode::SemiJoin {
                    input,
                    columns,
                    keys,
                } => {
                    let rel = self.input(&mut rels, v, *input)?;
                    self.keyed(v, *input, rel, columns, keys, &mut rels)?
                }
                PlanNode::Window {
                    input,
                    partition_by,
                    keys,
                } => {
                    let rel = self.input(&mut rels, v, *input)?;
                    let rel = match keys {
                        None => rel,
                        Some(k) => self.keyed(v, *input, rel, partition_by, k, &mut rels)?,
                    };
                    // A window computes over whole partitions: a node
                    // above narrows its read only by whole partitions.
                    let cols = self.dataset_columns(v, *input, partition_by)?;
                    match rel.side.clone() {
                        Some(mut side) => {
                            side.groups.push(cols);
                            rel.with_side(side)
                        }
                        None => rel,
                    }
                }
                PlanNode::Latest { input, columns, ts } | PlanNode::AsOf { input, columns, ts } => {
                    let rel = self.input(&mut rels, v, *input)?;
                    let asof = if matches!(node, PlanNode::AsOf { .. }) {
                        self.asof
                    } else {
                        None
                    };
                    let cols = self.dataset_columns(v, *input, columns)?;
                    let tcol = self.column_of(v, *input, ts)?.column;
                    match rel.side.clone() {
                        Some(side) => {
                            let s = self.select_versions(side, &cols, &tcol, asof)?;
                            rel.with_side(s)
                        }
                        None => Rel {
                            rows: Iv::new(0, rel.rows.hi),
                            ..rel
                        },
                    }
                }
                PlanNode::Join {
                    left,
                    right,
                    on,
                    kind,
                } => {
                    let l = self.input(&mut rels, v, *left)?;
                    let r = self.input(&mut rels, v, *right)?;
                    self.join(v, (*left, l), (*right, r), on, *kind)?
                }
                PlanNode::Aggregate {
                    input, group_by, ..
                } => {
                    let rel = self.input(&mut rels, v, *input)?;
                    self.aggregate(v, *input, rel, group_by)?
                }
                PlanNode::Exchange { input, .. } => {
                    let rel = self.input(&mut rels, v, *input)?;
                    moved = Some(self.moved(&rel)?);
                    rel
                }
                PlanNode::Project { input, .. } => self.input(&mut rels, v, *input)?,
            };
            let model = self.model_of(&rel);
            let bytes = match moved {
                Some(iv) => iv,
                None => Iv::exact(self.rel_bytes(&rel)?),
            };
            ops[v as usize] = Some(OperatorCost {
                node: v,
                op: op_name(node).to_string(),
                bytes: Self::quantity(bytes, model.as_ref()),
                rows_out: Self::quantity(rel.rows, model.as_ref()),
            });
            let consumers = self.checked.consumers(v);
            if consumers > 1 {
                let est = self.moved(&rel)?;
                shared.push(SharedOutput {
                    node: v,
                    consumers,
                    estimate: (est.lo, est.hi),
                });
            }
            rels[v as usize] = Some(rel);
        }
        let root = rels[plan.root as usize]
            .take()
            .ok_or_else(|| invalid(None, "the root was not priced"))?;
        let model = self.model_of(&root);
        let mut files = 0u64;
        let mut row_groups = 0u64;
        let mut bytes = 0u64;
        let mut reads = Vec::new();
        for (scan, (d, rgs)) in root.occurrences() {
            let b = self.read_bytes(scan, d, &rgs)?;
            files += self.read_files(d, &rgs)?;
            row_groups += rgs.len();
            bytes = bytes.saturating_add(b);
            reads.push(ScanRead {
                node: scan,
                dataset: d.to_string(),
                row_groups: rgs.iter().collect(),
                bytes: b,
            });
        }
        shared.sort_by_key(|s: &SharedOutput| s.node);
        let exactness = match &model {
            Some(m) => Exactness::Measured { model: m.clone() },
            None => Exactness::Exact,
        };
        let operators = ops
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| invalid(None, "a node was not priced"))?;
        Ok(PricedPlan {
            cost: RunCost {
                run_id: run_id.to_string(),
                keys: self.run_keys.len() as u64,
                files,
                row_groups,
                bytes,
                exact: model.is_none(),
                exactness,
                rows: Some(Self::quantity(root.rows, model.as_ref())),
                measured: None,
            },
            operators,
            reads,
            shared,
            model,
            unserved: self.unserved,
        })
    }

    /// The dataset columns `names` resolve to among `input`'s outputs.
    fn dataset_columns(
        &self,
        node: u32,
        input: u32,
        names: &[String],
    ) -> ReplayResult<Vec<String>> {
        names
            .iter()
            .map(|c| self.column_of(node, input, c).map(|c| c.column))
            .collect()
    }

    fn filter(
        &mut self,
        v: u32,
        input: u32,
        rel: Rel,
        p: &Predicate,
        rels: &mut Vec<Option<Rel>>,
    ) -> ReplayResult<Rel> {
        match p {
            Predicate::And { all } => {
                let mut rel = rel;
                for q in all {
                    rel = self.filter(v, input, rel, q, rels)?;
                }
                Ok(rel)
            }
            Predicate::In { columns, values } => self.keyed(
                v,
                input,
                rel,
                columns,
                &KeysFrom::Values {
                    values: values.clone(),
                },
                rels,
            ),
            Predicate::SemiJoin { columns, keys_from } => {
                self.keyed(v, input, rel, columns, keys_from, rels)
            }
            Predicate::Between { column, lo, hi } => match rel.side.clone() {
                Some(side) => {
                    let col = self.column_of(v, input, column)?.column;
                    let s = self.select_range(side, &col, lo, hi)?;
                    Ok(rel.with_side(s))
                }
                None => Ok(Rel {
                    rows: Iv::new(0, rel.rows.hi),
                    ..rel
                }),
            },
            _ => Err(invalid(Some(v), "this predicate is not priced")),
        }
    }

    /// `rel` kept to the rows whose `columns` tuple is among `keys`.
    fn keyed(
        &mut self,
        v: u32,
        input: u32,
        rel: Rel,
        columns: &[String],
        keys: &KeysFrom,
        rels: &mut Vec<Option<Rel>>,
    ) -> ReplayResult<Rel> {
        let Some(side) = rel.side.clone() else {
            // Rows derived from several scans: no index holds them.
            if let KeysFrom::Node { node, .. } = keys {
                let src = self.input(rels, v, *node)?;
                let mut reads = rel.reads;
                reads.extend(src.into_reads());
                return Ok(Rel {
                    side: None,
                    reads,
                    rows: Iv::new(0, rel.rows.hi),
                });
            }
            return Ok(Rel {
                rows: Iv::new(0, rel.rows.hi),
                ..rel
            });
        };
        let cols = self.dataset_columns(v, input, columns)?;
        if let Some(k) = self.keys_for(v, &side, &cols, keys)? {
            let s = self.select_keys(side, k)?;
            return Ok(rel.with_side(s));
        }
        // Keys from another node's output: a semi-join of two indexes.
        let KeysFrom::Node {
            node,
            columns: from,
        } = keys
        else {
            return Err(invalid(Some(v), "keys without a source"));
        };
        let src = self.input(rels, v, *node)?;
        // Keys from rows derived from several scans: matched through the
        // scan their columns come from, which bounds them.
        let sderived = src.side.is_none();
        let rside = match &src.side {
            Some(s) => s.clone(),
            None => self.proxy(v, "keys", *node, &src, from)?,
        };
        let rcols = self.dataset_columns(v, *node, from)?;
        let m = self.merge(v, ("left", &side, &cols), ("keys", &rside, &rcols))?;
        let in_rgs = self.rows_of(&side.dataset, &m.lrgs)?;
        let mut hi = side.rows.hi.min(in_rgs);
        if let Some(s) = m.lsum {
            hi = hi.min(s);
        }
        let rows = match (m.lpure && m.rpure, m.lsum) {
            (true, Some(s)) => Iv::exact(s),
            (true, None) => Iv::new(m.common, hi),
            (false, _) => Iv::new(0, hi),
        };
        let mut reads = rel.reads;
        if sderived {
            reads.extend(src.into_reads());
        } else {
            reads.push(Side {
                rgs: rside.read(Some(&rcols), m.rrgs.clone()),
                ..rside
            });
            reads.extend(src.reads);
        }
        Ok(Rel {
            side: Some(Side {
                rgs: side.read(Some(&cols), m.lrgs),
                rows,
                sel: Sel::Other,
                ..side
            }),
            reads,
            rows,
        })
    }

    /// The scan rows a derived input's (a join's, an aggregate's) column
    /// `names` take their values from: every read below `rel` of the one
    /// scan those columns resolve to, its row groups unioned, with no
    /// proof about which of its rows the derived rows hold. Every value the
    /// derived rows carry on those columns is a value of those rows, so its
    /// keys bound the derived keys from above; never its rows (a join
    /// below multiplies them).
    fn proxy(
        &self,
        v: u32,
        which: &'static str,
        input: u32,
        rel: &Rel,
        names: &[String],
    ) -> ReplayResult<Side> {
        let cols: Vec<Col> = names
            .iter()
            .map(|n| self.column_of(v, input, n))
            .collect::<ReplayResult<_>>()?;
        let scan = cols.first().map(|c| c.scan);
        let refused = || ReplayError::JoinNotIndexed {
            node: v,
            side: which,
            dataset: format!("node {input}"),
            columns: names.to_vec(),
            derived: Some(
                "those columns come from more than one scan below a join or an aggregate, so no \
                 one index holds their tuples"
                    .into(),
            ),
        };
        let Some(scan) = scan.filter(|s| cols.iter().all(|c| c.scan == *s)) else {
            return Err(refused());
        };
        let mut out: Option<Side> = None;
        for s in rel.sides().filter(|s| s.scan == scan) {
            match &mut out {
                Some(o) => o.rgs |= &s.rgs,
                None => {
                    out = Some(Side {
                        rows: Iv::new(0, rel.rows.hi),
                        sel: Sel::Other,
                        groups: Vec::new(),
                        ..s.clone()
                    });
                }
            }
        }
        out.ok_or_else(refused)
    }

    /// A two-sided join (module docs). A side that is rows derived from
    /// several scans (a join or an aggregate below) is matched through the
    /// scan its join columns come from ([`Pricer::proxy`]): it narrows the
    /// other side's read to the row groups holding a key of those rows,
    /// its own read stays as priced below, and the rows are bounded by the
    /// derived rows (never counted from the index).
    fn join(
        &mut self,
        v: u32,
        (li, l): (u32, Rel),
        (ri, r): (u32, Rel),
        on: &[(String, String)],
        kind: JoinKind,
    ) -> ReplayResult<Rel> {
        let lnames: Vec<String> = on.iter().map(|(a, _)| a.clone()).collect();
        let rnames: Vec<String> = on.iter().map(|(_, b)| b.clone()).collect();
        let (lderived, rderived) = (l.side.is_none(), r.side.is_none());
        let ls = match &l.side {
            Some(s) => s.clone(),
            None => self.proxy(v, "left", li, &l, &lnames)?,
        };
        let rs = match &r.side {
            Some(s) => s.clone(),
            None => self.proxy(v, "right", ri, &r, &rnames)?,
        };
        let lcols = self.dataset_columns(v, li, &lnames)?;
        let rcols = self.dataset_columns(v, ri, &rnames)?;
        let m = self.merge(v, ("left", &ls, &lcols), ("right", &rs, &rcols))?;
        let pure = m.lpure && m.rpure;
        // Rows of a side that can meet the other: a scan's rows are
        // bounded by the row groups and counts of the common keys; derived
        // rows only by their own interval.
        let side_hi = |s: &Side,
                       rel: &Rel,
                       derived: bool,
                       rgs: &RoaringBitmap,
                       sum: Option<u64>|
         -> ReplayResult<u64> {
            if derived {
                return Ok(rel.rows.hi);
            }
            let mut hi = s.rows.hi.min(self.rows_of(&s.dataset, rgs)?);
            if let Some(x) = sum {
                hi = hi.min(x);
            }
            Ok(hi)
        };
        let nl = side_hi(&ls, &l, lderived, &m.lrgs, m.lsum)?;
        let nr = side_hi(&rs, &r, rderived, &m.rrgs, m.rsum)?;
        let mut inner_hi = sat(u128::from(nl) * u128::from(nr));
        if let (Some(p), false, false) = (m.prod, lderived, rderived) {
            inner_hi = inner_hi.min(sat(p));
        }
        let inner = match (pure, m.prod) {
            (true, Some(p)) => Iv::exact(sat(p)),
            (true, None) => {
                let lo = m.common.max(m.lsum.unwrap_or(0)).max(m.rsum.unwrap_or(0));
                Iv::new(lo, inner_hi)
            }
            (false, _) => Iv::new(0, inner_hi),
        };
        let rows = match kind {
            JoinKind::Inner => inner,
            // Every left row is output at least once, matched or not.
            JoinKind::Left => match (pure, m.prod, m.lsum, l.rows.lo == l.rows.hi) {
                (true, Some(p), Some(s), true) => {
                    Iv::exact(sat(p).saturating_add(l.rows.hi.saturating_sub(s)))
                }
                _ => Iv::new(l.rows.lo, inner.hi.saturating_add(l.rows.hi)),
            },
        };
        let mut reads = Vec::new();
        if lderived {
            reads.extend(l.into_reads());
        } else {
            // A left join outputs every left row: its left side reads all
            // its row groups.
            let rgs = match kind {
                JoinKind::Inner => ls.read(Some(&lcols), m.lrgs.clone()),
                JoinKind::Left => ls.rgs.clone(),
            };
            reads.push(Side { rgs, ..ls });
            reads.extend(l.reads);
        }
        if rderived {
            reads.extend(r.into_reads());
        } else {
            let rgs = rs.read(Some(&rcols), m.rrgs);
            reads.push(Side { rgs, ..rs });
            reads.extend(r.reads);
        }
        Ok(Rel {
            side: None,
            reads,
            rows,
        })
    }

    fn aggregate(
        &mut self,
        v: u32,
        input: u32,
        rel: Rel,
        group_by: &[String],
    ) -> ReplayResult<Rel> {
        let groups = if group_by.is_empty() {
            // A global aggregate returns one row, on empty input too.
            Iv::exact(1)
        } else if let Some(side) = &rel.side {
            let cols = self.dataset_columns(v, input, group_by)?;
            self.groups(side, &cols)?
        } else {
            Iv::new(rel.rows.lo.min(1), rel.rows.hi)
        };
        Ok(Rel {
            side: None,
            reads: rel.into_reads(),
            rows: groups,
        })
    }

    /// The number of groups of `side` on `cols`.
    fn groups(&self, side: &Side, cols: &[String]) -> ReplayResult<Iv> {
        let fallback = Iv::new(side.rows.lo.min(1), side.rows.hi);
        let d = &side.dataset;
        // Both zeros of a float are one group in SQL and two stored keys,
        // so distinct keys only bound a float grouping from above.
        let schema = self.replayer(d)?.schema();
        let float = cols.iter().any(|c| {
            schema.field_with_name(c).is_ok_and(|f| {
                matches!(
                    ColumnType::of(f.data_type()),
                    Ok(ColumnType::Float16 | ColumnType::Float32 | ColumnType::Float64)
                )
            })
        });
        let Some(spec) = self.keyed_spec(d, cols, false) else {
            return Ok(fallback);
        };
        if float {
            return Ok(fallback);
        }
        match &side.sel {
            Sel::Keys(k) if same_set(k.columns(), cols) => {
                // Exactly the rows of these keys: one group per key held.
                let (positions, stored) = self.stored(d, &spec, k)?;
                let mut n = 0u64;
                for i in 0..k.len() {
                    n += u64::from(k.probes(i, &positions).iter().any(|p| {
                        stored
                            .get(p)
                            .is_some_and(|f| !(&f.bitmap & &side.rgs).is_empty())
                    }));
                }
                Ok(Iv::exact(n))
            }
            sel => {
                let segs = self.replayer(d)?.segments_of(&spec)?;
                let distinct = bellwether_core::count_live_keys(segs.readers(), &side.rgs)?;
                // Rows whose leading key column is null are not indexed
                // (ADR 014): with one column they form one group; with
                // more, between one and their number.
                let lead = keyed_columns(&spec)
                    .and_then(|c| c.first())
                    .cloned()
                    .unwrap_or_default();
                let c = self.replayer(d)?.catalog();
                let (mut nulls, mut unknown) = (0u64, 0u64);
                for id in &side.rgs {
                    if let Some(rg) = c.get(id) {
                        match rg
                            .meta
                            .stats
                            .iter()
                            .find(|s| s.column == lead)
                            .and_then(|s| s.null_count)
                        {
                            Some(x) => nulls += x,
                            None => unknown += rg.meta.rows,
                        }
                    }
                }
                let extra_lo = u64::from(nulls > 0);
                let extra_hi = if cols.len() == 1 {
                    u64::from(nulls + unknown > 0)
                } else {
                    nulls + unknown
                };
                if matches!(sel, Sel::All) {
                    Ok(Iv::new(distinct + extra_lo, distinct + extra_hi))
                } else {
                    // A subset of the rows: each group a key held here or
                    // the null group, and at most one per row.
                    Ok(Iv::new(
                        side.rows.lo.min(1),
                        side.rows.hi.min(distinct + extra_hi),
                    ))
                }
            }
        }
    }

    /// The compressed-equivalent bytes of `rel`'s rows: its rows times the
    /// average compressed width of a row over each side's planned row
    /// groups, rounded outward. A side reached by two paths (a self-join)
    /// carries its columns twice in an output row, so it counts per path.
    /// Bounded with respect to that definition (the rows are an interval
    /// that holds the truth); the width is a model of the in-memory width,
    /// which no statistic gives.
    fn moved(&self, rel: &Rel) -> ReplayResult<Iv> {
        let (mut lo, mut hi) = (0u128, 0u128);
        for s in rel.sides() {
            let bytes = u128::from(self.read_bytes(s.scan, &s.dataset, &s.rgs)?);
            let rows = u128::from(self.rows_of(&s.dataset, &s.rgs)?);
            if rows == 0 {
                continue;
            }
            lo += u128::from(rel.rows.lo) * bytes / rows;
            hi += (u128::from(rel.rows.hi) * bytes).div_ceil(rows);
        }
        Ok(Iv::new(sat(lo), sat(hi)))
    }
}

/// One stored key's row groups, per-row-group counts, and versions,
/// folded across the segments that hold it.
#[derive(Clone, Debug, Default)]
struct Folded {
    bitmap: RoaringBitmap,
    /// Rows per row group, when every segment holding the key counts.
    counts: Option<BTreeMap<u32, u64>>,
    seen: bool,
    versions: Vec<(i64, u32)>,
}

impl Folded {
    fn of(v: &KeyValue) -> Self {
        let mut f = Self::default();
        f.add(v);
        f
    }

    fn add(&mut self, v: &KeyValue) {
        let counts: Option<BTreeMap<u32, u64>> = v
            .counts
            .as_ref()
            .map(|c| v.bitmap.iter().zip(c.iter().copied()).collect());
        self.counts = match (self.seen, self.counts.take(), counts) {
            (false, _, c) => c,
            (true, Some(mut a), Some(b)) => {
                a.extend(b);
                Some(a)
            }
            _ => None,
        };
        self.seen = true;
        self.bitmap |= &v.bitmap;
        self.versions
            .extend(v.chain.versions().iter().map(|(t, rg)| (*t, rg.0)));
    }

    /// Rows of the key in `rgs`, when counted.
    fn rows_in(&self, rgs: &RoaringBitmap) -> Option<u64> {
        self.counts.as_ref().map(|c| {
            c.iter()
                .filter(|(rg, _)| rgs.contains(**rg))
                .map(|(_, n)| *n)
                .fold(0u64, u64::saturating_add)
        })
    }
}
