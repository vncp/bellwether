//! Hooks for counterfactual attribution (ADR 024). This module does not
//! implement the optimizer (WP13); it provides the two exact replays the
//! optimizer's backward pass is built from.
//!
//! A plan here is a configuration's ordered list of pruning nodes, one key
//! spec each; a run's plan intersects the nodes that serve it per key tuple.
//! For node `v`, [`Replayer::replace_node`] replays the plan with `v`
//! replaced by an alternative spec, or ablated, and returns per run
//! `Δ_v = c(a) − c(a ⊖ v)` for files, row groups, and bytes. For a pair,
//! [`Replayer::interaction`] returns
//! `I_uv = c(a_uv) − c(a_u) − c(a_v) + c(a)`: nonzero means the two choices
//! do not compose additively. Every term is an exact replay; nothing is
//! sampled or estimated.

use bellwether_types::{Error, KeySpec, Result, RunCost};
use serde::{Deserialize, Serialize};

use crate::replay::Replayer;
use crate::workload::{KeySetSource, Workload};

/// What replaces a node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Alternative {
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
fn apply(nodes: &[KeySpec], changes: &[(usize, &Alternative)]) -> Result<Vec<KeySpec>> {
    let mut out: Vec<Option<KeySpec>> = nodes.iter().cloned().map(Some).collect();
    for (i, alt) in changes {
        let slot = out
            .get_mut(*i)
            .ok_or_else(|| Error::Other(format!("node {i} is out of range")))?;
        *slot = match alt {
            Alternative::Replace(s) => Some(s.clone()),
            Alternative::Ablate => None,
        };
    }
    Ok(out.into_iter().flatten().collect())
}

impl Replayer<'_> {
    /// Replays the plan `nodes` and the plan with `nodes[node]` replaced by
    /// `alternative`, and returns `Δ_v` per run, in workload order.
    pub fn replace_node(
        &self,
        nodes: &[KeySpec],
        node: usize,
        alternative: &Alternative,
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<Vec<RunDelta>> {
        let base = self.replay_specs(nodes, workload, keys)?;
        let changed = apply(nodes, &[(node, alternative)])?;
        let cf = self.replay_specs(&changed, workload, keys)?;
        Ok(base
            .into_iter()
            .zip(cf)
            .map(|(b, c)| RunDelta {
                run_id: b.run_id.clone(),
                delta: CostDelta::between(&b, &c),
                base: b,
                counterfactual: c,
            })
            .collect())
    }

    /// The pairwise interaction of changing node `u` and node `v`, per run:
    /// four exact replays.
    pub fn interaction(
        &self,
        nodes: &[KeySpec],
        u: (usize, &Alternative),
        v: (usize, &Alternative),
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<Vec<RunInteraction>> {
        if u.0 == v.0 {
            return Err(Error::Other(
                "an interaction needs two different nodes".into(),
            ));
        }
        let plain = self.replay_specs(nodes, workload, keys)?;
        let with_u = self.replay_specs(&apply(nodes, &[u])?, workload, keys)?;
        let with_v = self.replay_specs(&apply(nodes, &[v])?, workload, keys)?;
        let with_both = self.replay_specs(&apply(nodes, &[u, v])?, workload, keys)?;
        Ok(plain
            .iter()
            .zip(&with_u)
            .zip(&with_v)
            .zip(&with_both)
            .map(|(((base, cu), cv), cuv)| RunInteraction {
                run_id: base.run_id.clone(),
                // (c(a_uv) − c(a_u)) + (c(a) − c(a_v))
                interaction: CostDelta::between(cuv, cu).plus(CostDelta::between(base, cv)),
            })
            .collect())
    }
}
