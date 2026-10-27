//! Consumer plans (relational replay, WP15): the datasets a plan scans
//! besides the data root, bound by name by whoever runs the engine
//! ([`crate::EngineOptions::datasets`]: `--dataset NAME=ROOT`), each read
//! through its own index at its own pinned manifest; and the per-node
//! costs `why --plan` reports, summed over runs.
//!
//! A tool argument names datasets only from the bindings; a name the
//! operator did not bind is refused before any folder is read, so an
//! agent cannot open a root of its choosing (threat model).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use bellwether_replay::{load_plan, KeySetSource, Replayer, Workload, MAIN_DATASET};
use bellwether_types::{
    ConsumerPlan, Error, Exactness, Hash, OperatorCost, PlanNode, ProposalBody, Quantity, Result,
    Source,
};
use serde::{Deserialize, Serialize};

use crate::engine::{Engine, EngineOptions, Pinned};

/// One dataset a plan scans, as the replay read it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDataset {
    /// The name the plan gives it.
    pub name: String,
    /// Its root (the data root for `main` and an alias of it; a table's
    /// URL without credentials).
    pub root: String,
    /// The manifest sequence it was pinned to (0 for a folder never
    /// indexed).
    pub sequence: u64,
    /// The model its costs are measured by, for a table (ADR 027).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

/// What `why --plan` reports beside the funnel: the plan, the datasets it
/// read, and each node's cost summed over the runs, under the current
/// configuration and under the recommended key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanReport {
    /// The plan's BLAKE3 (its canonical JSON, cached beside the key sets;
    /// every run of the workload names it).
    pub plan: Hash,
    /// The datasets bound for it, `main` first.
    pub datasets: Vec<PlanDataset>,
    /// Each node's cost summed over the runs under the current
    /// configuration, in plan order. A sum of exact quantities is exact; a
    /// bounded one's interval is the sum of the intervals.
    pub current: Vec<OperatorCost>,
    /// The same under the recommended key, when one is recommended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended: Option<Vec<OperatorCost>>,
}

/// The datasets a set of plans scans besides the data root: name, root,
/// and engine.
pub(crate) struct PlanDatasets {
    held: Vec<Held>,
    aliases: Vec<String>,
}

struct Held {
    name: String,
    engine: Engine,
    pinned: Option<Pinned>,
    source: Box<dyn Source>,
}

/// The dataset names `plan` scans besides `main`.
pub(crate) fn scanned(plan: &ConsumerPlan) -> BTreeSet<String> {
    plan.nodes
        .iter()
        .filter_map(|n| match n {
            PlanNode::Scan { dataset, .. } if dataset != MAIN_DATASET => Some(dataset.clone()),
            _ => None,
        })
        .collect()
}

/// The plans `workload`'s runs name, loaded from the cache, each once.
pub(crate) fn workload_plans(
    workload: &Workload,
    keys: &dyn KeySetSource,
) -> Result<Vec<(Hash, ConsumerPlan)>> {
    let hashes: BTreeSet<Hash> = workload.runs().iter().filter_map(|r| r.plan).collect();
    hashes
        .into_iter()
        .map(|h| Ok((h, load_plan(keys, &h)?)))
        .collect()
}

/// The plans a proposal and a workload price: a rewrite's two, else the
/// workload's.
pub(crate) fn proposal_plans(
    body: &ProposalBody,
    workload: Option<&Workload>,
    keys: &dyn KeySetSource,
) -> Result<Vec<ConsumerPlan>> {
    if let ProposalBody::Rewrite { shape } = body {
        return Ok(shape
            .before_plan
            .iter()
            .chain(&shape.after_plan)
            .cloned()
            .collect());
    }
    match workload {
        Some(w) => Ok(workload_plans(w, keys)?
            .into_iter()
            .map(|(_, p)| p)
            .collect()),
        None => Ok(Vec::new()),
    }
}

/// Reads a plan file (read only): a consumer plan's JSON.
pub(crate) fn read_plan(path: &PathBuf) -> Result<ConsumerPlan> {
    let bytes = std::fs::read(path).map_err(|e| Error::InvalidArgument {
        field: "plan",
        reason: format!("cannot read the plan file {} ({e})", path.display()),
    })?;
    serde_json::from_slice(&bytes).map_err(|e| Error::InvalidArgument {
        field: "plan",
        reason: format!(
            "{} is not a consumer plan ({e}); a plan is JSON {{\"version\": 1, \"nodes\": [...], \
             \"root\": N}} with nodes such as {{\"op\": \"scan\", \"dataset\": \"main\", \
             \"columns\": [...]}} (docs/evolve.md section 14)",
            path.display()
        ),
    })
}

impl Engine {
    /// Opens the datasets `names` binds ([`EngineOptions::datasets`]),
    /// each through its own index, in this engine's format and clock; a
    /// binding to this engine's root is `main` under another name. A name
    /// not bound is refused before any folder is read.
    pub(crate) fn plan_datasets(&self, names: &BTreeSet<String>) -> Result<PlanDatasets> {
        let mut held = Vec::new();
        let mut aliases = Vec::new();
        for name in names {
            let Some(root) = self.options().datasets.get(name) else {
                let bound: Vec<&str> = self.options().datasets.keys().map(String::as_str).collect();
                return Err(Error::InvalidArgument {
                    field: "datasets",
                    reason: format!(
                        "the plan scans dataset {name:?}, which is not bound ({}); bind it to its \
                         folder with --dataset {name}=<folder> (an MCP server binds its datasets \
                         when the human starts it, never from a tool argument)",
                        if bound.is_empty() {
                            "none is".to_string()
                        } else {
                            format!("bound: {}", bound.join(", "))
                        }
                    ),
                });
            };
            let options = EngineOptions {
                format: self.options().format,
                clock: self.options().clock.clone(),
                threads: self.options().threads,
                ..EngineOptions::default()
            };
            let engine = Engine::open(root, options)?;
            if engine.root() == self.root() {
                aliases.push(name.clone());
                continue;
            }
            let pinned = engine.pin()?;
            let source = engine.source(pinned.as_ref())?;
            held.push(Held {
                name: name.clone(),
                engine,
                pinned,
                source,
            });
        }
        Ok(PlanDatasets { held, aliases })
    }

    /// The datasets the request names and the plans scan: every name must
    /// be bound; with names given, the plans scan only those.
    pub(crate) fn datasets_for(
        &self,
        asked: &[String],
        plans: &[ConsumerPlan],
    ) -> Result<PlanDatasets> {
        for a in asked {
            if !self.options().datasets.contains_key(a) {
                return Err(Error::InvalidArgument {
                    field: "datasets",
                    reason: format!(
                        "dataset {a:?} is not bound; bind it with --dataset {a}=<folder> (an MCP \
                         server binds its datasets when the human starts it)"
                    ),
                });
            }
        }
        let mut names = BTreeSet::new();
        for p in plans {
            for n in scanned(p) {
                if !asked.is_empty() && !asked.contains(&n) {
                    return Err(Error::InvalidArgument {
                        field: "datasets",
                        reason: format!(
                            "the plan scans dataset {n:?}, which the request does not name \
                             (datasets: {}); add it",
                            asked.join(", ")
                        ),
                    });
                }
                names.insert(n);
            }
        }
        self.plan_datasets(&names)
    }
}

impl PlanDatasets {
    /// No datasets.
    pub(crate) fn none() -> Self {
        Self {
            held: Vec::new(),
            aliases: Vec::new(),
        }
    }

    /// `main` with every dataset attached, each a replayer over its own
    /// pinned index (a table's measured by its aggregates).
    pub(crate) fn attach<'a>(&'a self, main: Replayer<'a>) -> Result<Replayer<'a>> {
        let mut main = main;
        for a in &self.aliases {
            main = main.with_main_alias(a)?;
        }
        if self.held.is_empty() {
            return Ok(main);
        }
        let mut map = BTreeMap::new();
        for h in &self.held {
            let r = crate::fix::replayer(&h.engine, &*h.source, h.pinned.as_ref())?;
            map.insert(h.name.clone(), r);
        }
        main.with_datasets(map)
    }

    /// The datasets as the report names them, `main` (and its aliases)
    /// first.
    pub(crate) fn describe(&self, main: &Engine) -> Result<Vec<PlanDataset>> {
        let mut out = Vec::new();
        let seq = |e: &Engine| -> Result<u64> { Ok(e.manifest()?.map_or(0, |m| m.sequence)) };
        let model = |e: &Engine| {
            e.is_table()
                .then(|| bellwether_source_sql::MEASURED_AGGREGATES.to_string())
        };
        for name in std::iter::once(MAIN_DATASET.to_string()).chain(self.aliases.iter().cloned()) {
            out.push(PlanDataset {
                name,
                root: shown(main.root()),
                sequence: seq(main)?,
                model: model(main),
            });
        }
        for h in &self.held {
            out.push(PlanDataset {
                name: h.name.clone(),
                root: shown(h.engine.root()),
                sequence: h.pinned.as_ref().map_or(0, Pinned::sequence),
                model: model(&h.engine),
            });
        }
        Ok(out)
    }
}

fn shown(root: &str) -> String {
    if crate::is_sql_root(root) {
        crate::redact_db(root)
    } else {
        root.to_string()
    }
}

/// Each node's cost summed over runs: blocks of one plan's nodes, in plan
/// order. Exact plus exact is exact; a bounded term widens the sum to the
/// sum of the intervals; a measured term makes it measured.
pub(crate) fn sum_operators<'o>(blocks: impl IntoIterator<Item = &'o [OperatorCost]>) -> Vec<OperatorCost> {
    let mut out: Vec<OperatorCost> = Vec::new();
    for block in blocks {
        if out.is_empty() {
            out = block.to_vec();
            continue;
        }
        for (o, b) in out.iter_mut().zip(block) {
            o.bytes = add(&o.bytes, &b.bytes);
            o.rows_out = add(&o.rows_out, &b.rows_out);
        }
    }
    out
}

/// `a + b` for quantities.
pub(crate) fn add(a: &Quantity, b: &Quantity) -> Quantity {
    let bounds = |q: &Quantity| match &q.exactness {
        Exactness::Bounded { low, high } => (*low, *high),
        _ => (q.value, q.value),
    };
    let ((al, ah), (bl, bh)) = (bounds(a), bounds(b));
    let value = a.value.saturating_add(b.value);
    let exactness = match (&a.exactness, &b.exactness) {
        (Exactness::Measured { model }, _) | (_, Exactness::Measured { model }) => {
            Exactness::Measured {
                model: model.clone(),
            }
        }
        (Exactness::Exact, Exactness::Exact) => Exactness::Exact,
        _ => Exactness::Bounded {
            low: al.saturating_add(bl),
            high: ah.saturating_add(bh),
        },
    };
    Quantity { value, exactness }
}
