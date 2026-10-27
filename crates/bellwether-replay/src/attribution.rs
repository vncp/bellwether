//! Hooks for counterfactual attribution (ADR 024). This module does not
//! implement the optimizer (WP13); it provides the exact replays the
//! optimizer's backward pass is built from, over two kinds of node.
//!
//! - **Plan nodes** (relational replay, WP15): a consumer's
//!   [`ConsumerPlan`]. For node `v`, [`Replayer::replace_node`] replays
//!   every run through the plan and through the plan with `v` replaced by
//!   another node, or ablated (its consumers read its input instead), under
//!   the current configuration, and returns per run `Δ_v = c(a) − c(a ⊖ v)`
//!   for files, row groups, and bytes. [`Replayer::interaction`] returns
//!   `I_uv = c(a_uv) − c(a_u) − c(a_v) + c(a)` for two nodes: nonzero means
//!   the two changes do not compose additively.
//! - **Pruning specs**: a configuration's ordered list of key specs, one
//!   node each; a run's plan intersects the specs that serve it per key
//!   tuple. [`Replayer::replace_spec`] and [`Replayer::spec_interaction`]
//!   are the same two replays with a spec replaced or removed.
//!
//! Every term is an exact replay; nothing is sampled or estimated.

use bellwether_types::{
    ConsumerPlan, Error, KeySpec, KeysFrom, PlanNode, Predicate, Result, RunCost,
};
use serde::{Deserialize, Serialize};

use crate::replay::Replayer;
use crate::workload::{load_plan, KeySetSource, Workload};

/// What replaces a plan node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Alternative {
    /// The node becomes this node (its inputs index the same plan).
    Replace(PlanNode),
    /// The node is removed: every node that read it reads its input. Only a
    /// node with one input (not a scan or a join) can be removed.
    Ablate,
}

/// What replaces a pruning spec.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpecAlternative {
    /// The node becomes this spec.
    Replace(KeySpec),
    /// The node is removed.
    Ablate,
}

/// A signed difference of two run costs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CostDelta {
    /// Files.
    pub files: i128,
    /// Row groups.
    pub row_groups: i128,
    /// Bytes.
    pub bytes: i128,
}

impl CostDelta {
    /// `a − b`, component-wise.
    #[must_use]
    pub fn between(a: &RunCost, b: &RunCost) -> Self {
        Self {
            files: i128::from(a.files) - i128::from(b.files),
            row_groups: i128::from(a.row_groups) - i128::from(b.row_groups),
            bytes: i128::from(a.bytes) - i128::from(b.bytes),
        }
    }

    fn plus(self, o: Self) -> Self {
        Self {
            files: self.files + o.files,
            row_groups: self.row_groups + o.row_groups,
            bytes: self.bytes + o.bytes,
        }
    }
}

/// One run's attribution to a node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunDelta {
    /// The run.
    pub run_id: String,
    /// `c(a)`: the run under the plan.
    pub base: RunCost,
    /// `c(a ⊖ v)`: the run with the node replaced or ablated.
    pub counterfactual: RunCost,
    /// `Δ_v = c(a) − c(a ⊖ v)`.
    pub delta: CostDelta,
}

/// One run's pairwise interaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunInteraction {
    /// The run.
    pub run_id: String,
    /// `I_uv = c(a_uv) − c(a_u) − c(a_v) + c(a)`.
    pub interaction: CostDelta,
}

/// `nodes` with each `(index, alternative)` applied.
fn apply_specs(nodes: &[KeySpec], changes: &[(usize, &SpecAlternative)]) -> Result<Vec<KeySpec>> {
    let mut out: Vec<Option<KeySpec>> = nodes.iter().cloned().map(Some).collect();
    for (i, alt) in changes {
        let slot = out
            .get_mut(*i)
            .ok_or_else(|| Error::Other(format!("node {i} is out of range")))?;
        *slot = match alt {
            SpecAlternative::Replace(s) => Some(s.clone()),
            SpecAlternative::Ablate => None,
        };
    }
    Ok(out.into_iter().flatten().collect())
}

/// `node` with every node index it reads mapped through `f`.
fn remap(node: &PlanNode, f: &dyn Fn(u32) -> u32) -> PlanNode {
    fn keys(k: &KeysFrom, f: &dyn Fn(u32) -> u32) -> KeysFrom {
        match k {
            KeysFrom::Node { node, columns } => KeysFrom::Node {
                node: f(*node),
                columns: columns.clone(),
            },
            other => other.clone(),
        }
    }
    fn predicate(p: &Predicate, f: &dyn Fn(u32) -> u32) -> Predicate {
        match p {
            Predicate::SemiJoin { columns, keys_from } => Predicate::SemiJoin {
                columns: columns.clone(),
                keys_from: keys(keys_from, f),
            },
            Predicate::And { all } => Predicate::And {
                all: all.iter().map(|q| predicate(q, f)).collect(),
            },
            other => other.clone(),
        }
    }
    let mut n = node.clone();
    match &mut n {
        PlanNode::Scan { .. } => {}
        PlanNode::Filter {
            input,
            predicate: p,
        } => {
            *input = f(*input);
            *p = predicate(p, f);
        }
        PlanNode::Latest { input, .. }
        | PlanNode::AsOf { input, .. }
        | PlanNode::Aggregate { input, .. }
        | PlanNode::Exchange { input, .. }
        | PlanNode::Project { input, .. } => *input = f(*input),
        PlanNode::SemiJoin { input, keys: k, .. } => {
            *input = f(*input);
            *k = keys(k, f);
        }
        PlanNode::Window { input, keys: k, .. } => {
            *input = f(*input);
            *k = k.as_ref().map(|k| keys(k, f));
        }
        PlanNode::Join { left, right, .. } => {
            *left = f(*left);
            *right = f(*right);
        }
    }
    n
}

/// `plan` with node `v` replaced or ablated ([`apply_all`]).
pub fn apply(plan: &ConsumerPlan, v: u32, alternative: &Alternative) -> Result<ConsumerPlan> {
    apply_all(plan, &[(v, alternative)])
}

/// `plan` with each `(node, alternative)` applied at once, every index
/// naming a node of `plan`. An ablated node's consumers read its input (an
/// input ablated too is passed through in turn); nodes no longer read by
/// the root (an ablated node, and any node only its keys came from) are
/// dropped and the rest renumbered in order.
pub fn apply_all(plan: &ConsumerPlan, changes: &[(u32, &Alternative)]) -> Result<ConsumerPlan> {
    let mut nodes = plan.nodes.clone();
    let mut through: Vec<Option<u32>> = vec![None; nodes.len()];
    for (v, alternative) in changes {
        let node = plan
            .nodes
            .get(*v as usize)
            .ok_or_else(|| Error::Other(format!("node {v} is out of range")))?;
        match alternative {
            Alternative::Replace(n) => nodes[*v as usize] = n.clone(),
            Alternative::Ablate => {
                through[*v as usize] = Some(match node {
                    PlanNode::Filter { input, .. }
                    | PlanNode::Latest { input, .. }
                    | PlanNode::AsOf { input, .. }
                    | PlanNode::SemiJoin { input, .. }
                    | PlanNode::Window { input, .. }
                    | PlanNode::Aggregate { input, .. }
                    | PlanNode::Exchange { input, .. }
                    | PlanNode::Project { input, .. } => *input,
                    PlanNode::Scan { .. } | PlanNode::Join { .. } => {
                        return Err(Error::InvalidArgument {
                            field: "alternative",
                            reason: format!(
                                "node {v} is a {}; removing it leaves its consumer no input, so \
                                 replace it with another node instead",
                                crate::relational::op_name(node)
                            ),
                        })
                    }
                });
            }
        }
    }
    // Follow ablated nodes to the first kept input; a chain longer than the
    // plan is a cycle.
    let resolve = |mut u: u32| -> u32 {
        for _ in 0..=through.len() {
            match through.get(u as usize).copied().flatten() {
                Some(next) => u = next,
                None => return u,
            }
        }
        u32::MAX
    };
    let nodes: Vec<PlanNode> = nodes.iter().map(|n| remap(n, &resolve)).collect();
    let root = resolve(plan.root);
    // Keep the nodes the root reads, renumbered in order.
    let mut keep = vec![false; nodes.len()];
    let mut stack = vec![root];
    while let Some(u) = stack.pop() {
        let Some(slot) = keep.get_mut(u as usize) else {
            return Err(Error::Other(format!("node {u} is out of range")));
        };
        if !*slot {
            *slot = true;
            stack.extend(crate::relational::children(&nodes[u as usize]));
        }
    }
    let mut index = vec![u32::MAX; nodes.len()];
    let mut next = 0u32;
    for (i, k) in keep.iter().enumerate() {
        if *k {
            index[i] = next;
            next += 1;
        }
    }
    let renumber = |u: u32| index.get(u as usize).copied().unwrap_or(u32::MAX);
    Ok(ConsumerPlan {
        version: plan.version,
        nodes: nodes
            .iter()
            .zip(&keep)
            .filter(|(_, k)| **k)
            .map(|(n, _)| remap(n, &renumber))
            .collect(),
        root: renumber(root),
    })
}

impl Replayer<'_> {
    /// Every run of `workload` priced through `plan` under the current
    /// configuration, the run's keys and instant feeding it.
    pub fn replay_plan(
        &self,
        plan: &ConsumerPlan,
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<Vec<RunCost>> {
        let checked = self.check_plan(plan).map_err(Error::from)?;
        workload
            .runs()
            .iter()
            .map(|run| {
                let rk = workload.run_keys(keys, run)?;
                let at = run.query()?.asof_ts();
                Ok(self
                    .price_checked(&checked, self.current_specs(), &run.run_id, &rk, at)
                    .map_err(Error::from)?
                    .cost)
            })
            .collect()
    }

    /// Replays every run through `plan` and through `plan` with node `node`
    /// replaced by `alternative`, under the current configuration, and
    /// returns `Δ_v` per run, in workload order.
    pub fn replace_node(
        &self,
        plan: &ConsumerPlan,
        node: u32,
        alternative: &Alternative,
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<Vec<RunDelta>> {
        let base = self.replay_plan(plan, workload, keys)?;
        let cf = self.replay_plan(&apply(plan, node, alternative)?, workload, keys)?;
        Ok(deltas(base, cf))
    }

    /// The pairwise interaction of changing plan nodes `u` and `v`, per run:
    /// four exact replays, every change made on `plan`'s own numbering
    /// ([`apply_all`]).
    pub fn interaction(
        &self,
        plan: &ConsumerPlan,
        u: (u32, &Alternative),
        v: (u32, &Alternative),
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<Vec<RunInteraction>> {
        if u.0 == v.0 {
            return Err(Error::Other(
                "an interaction needs two different nodes".into(),
            ));
        }
        let plain = self.replay_plan(plan, workload, keys)?;
        let with_u = self.replay_plan(&apply(plan, u.0, u.1)?, workload, keys)?;
        let with_v = self.replay_plan(&apply(plan, v.0, v.1)?, workload, keys)?;
        let with_both = self.replay_plan(&apply_all(plan, &[u, v])?, workload, keys)?;
        Ok(interactions(&plain, &with_u, &with_v, &with_both))
    }

    /// [`Replayer::replace_node`] for the plan a run of `workload` names
    /// (every run must name the same one).
    pub fn replace_workload_node(
        &self,
        node: u32,
        alternative: &Alternative,
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<Vec<RunDelta>> {
        let plan = workload_plan(workload, keys)?;
        self.replace_node(&plan, node, alternative, workload, keys)
    }

    /// Replays the configuration `nodes` and the configuration with
    /// `nodes[node]` replaced by `alternative`, and returns `Δ_v` per run,
    /// in workload order.
    pub fn replace_spec(
        &self,
        nodes: &[KeySpec],
        node: usize,
        alternative: &SpecAlternative,
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<Vec<RunDelta>> {
        let base = self.replay_specs(nodes, workload, keys)?;
        let changed = apply_specs(nodes, &[(node, alternative)])?;
        let cf = self.replay_specs(&changed, workload, keys)?;
        Ok(deltas(base, cf))
    }

    /// The pairwise interaction of changing spec `u` and spec `v`, per run:
    /// four exact replays.
    pub fn spec_interaction(
        &self,
        nodes: &[KeySpec],
        u: (usize, &SpecAlternative),
        v: (usize, &SpecAlternative),
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<Vec<RunInteraction>> {
        if u.0 == v.0 {
            return Err(Error::Other(
                "an interaction needs two different nodes".into(),
            ));
        }
        let plain = self.replay_specs(nodes, workload, keys)?;
        let with_u = self.replay_specs(&apply_specs(nodes, &[u])?, workload, keys)?;
        let with_v = self.replay_specs(&apply_specs(nodes, &[v])?, workload, keys)?;
        let with_both = self.replay_specs(&apply_specs(nodes, &[u, v])?, workload, keys)?;
        Ok(interactions(&plain, &with_u, &with_v, &with_both))
    }
}

fn workload_plan(workload: &Workload, keys: &dyn KeySetSource) -> Result<ConsumerPlan> {
    let mut hashes = workload.runs().iter().map(|r| r.plan);
    let first = hashes
        .next()
        .flatten()
        .ok_or_else(|| Error::InvalidArgument {
            field: "workload",
            reason: "the workload's runs name no consumer plan; record them with `why --plan`"
                .into(),
        })?;
    if hashes.any(|h| h != Some(first)) {
        return Err(Error::InvalidArgument {
            field: "workload",
            reason: "the workload's runs name different consumer plans; attribute one plan at a \
                     time"
                .into(),
        });
    }
    load_plan(keys, &first)
}

fn deltas(base: Vec<RunCost>, cf: Vec<RunCost>) -> Vec<RunDelta> {
    base.into_iter()
        .zip(cf)
        .map(|(b, c)| RunDelta {
            run_id: b.run_id.clone(),
            delta: CostDelta::between(&b, &c),
            base: b,
            counterfactual: c,
        })
        .collect()
}

fn interactions(
    plain: &[RunCost],
    with_u: &[RunCost],
    with_v: &[RunCost],
    with_both: &[RunCost],
) -> Vec<RunInteraction> {
    plain
        .iter()
        .zip(with_u)
        .zip(with_v)
        .zip(with_both)
        .map(|(((base, cu), cv), cuv)| RunInteraction {
            run_id: base.run_id.clone(),
            // (c(a_uv) − c(a_u)) + (c(a) − c(a_v))
            interaction: CostDelta::between(cuv, cu).plus(CostDelta::between(base, cv)),
        })
        .collect()
}
