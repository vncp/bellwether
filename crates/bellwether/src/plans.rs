//! Consumer plans (relational replay, WP15): the datasets a plan scans
//! besides the data root, bound by name by whoever runs the engine
//! ([`crate::EngineOptions::datasets`]: `--dataset NAME=ROOT`), each read
//! through its own index at its own pinned manifest; and the per-node
//! costs `why --plan` reports, summed over runs.
//!
//! A tool argument names datasets only from the bindings; a name the
//! operator did not bind is refused before any folder is read, so an
//! agent cannot open a root of its choosing (threat model).
//!
//! A plan is a DAG and the convention is logical reuse
//! (`bellwether_replay::relational`): every node evaluated once per run, a
//! scan read by several paths read once with the union of their row
//! groups. That is a statement about the plan, not a claim that an engine
//! buffers or reuses a shared node; the buffered output's size is a model
//! estimate ([`SharedEstimate`]), and memory stays unmeasured.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use bellwether_replay::{load_plan, KeySetSource, Replayer, Workload, MAIN_DATASET};
use bellwether_types::{
    ConsumerPlan, Error, Exactness, Hash, KeySpec, OperatorCost, PlanNode, Quantity, Result, Source,
};
use serde::{Deserialize, Serialize};

use crate::engine::{Engine, EngineOptions, Pinned};

/// What the buffered output of a shared node is estimated by.
pub const SHARED_ESTIMATE_MODEL: &str = "model: rows_out x average compressed row width of the \
                                         planned row groups; not a bound on runtime memory, \
                                         which stays unmeasured";

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

/// What `why --plan` reports beside the funnel for one plan: the datasets
/// it read and each node's cost summed over the runs that name it, under
/// the current configuration and under the recommended key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanReport {
    /// The plan's BLAKE3 (its canonical JSON, cached beside the key sets;
    /// every run it prices names it).
    pub plan: Hash,
    /// The runs priced through it.
    pub runs: u64,
    /// The datasets bound for it, `main` first.
    pub datasets: Vec<PlanDataset>,
    /// Per node, in plan order: how many times nodes read it (2 or more:
    /// a shared node, evaluated once; 0: the root).
    pub consumers: Vec<u32>,
    /// Each node's cost summed over the runs under the current
    /// configuration, in plan order. A sum of exact quantities is exact; a
    /// bounded one's interval is the sum of the intervals; a node over a
    /// measured dataset is measured.
    pub current: Vec<OperatorCost>,
    /// The same under the recommended key, when one is recommended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended: Option<Vec<OperatorCost>>,
    /// Each read occurrence (a `scan` node, read once per run), summed
    /// over the runs under the current configuration.
    pub reads: Vec<PlanRead>,
    /// Each shared node with its model estimate under the current
    /// configuration, summed over the runs. Omitted when none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shared: Vec<SharedEstimate>,
}

/// One read occurrence of a plan, summed over runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanRead {
    /// The scan node.
    pub node: u32,
    /// Its dataset.
    pub dataset: String,
    /// Row groups read, summed over runs.
    pub row_groups: u64,
    /// Projected compressed bytes read, summed over runs (exact over
    /// files).
    pub bytes: u64,
}

/// A node several nodes read (logical reuse: evaluated once per run), and
/// a compressed-equivalent model estimate of the output its consumers
/// share, summed over runs. Not a measurement and not a bound on memory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedEstimate {
    /// The node.
    pub node: u32,
    /// How many times nodes read it.
    pub consumers: u32,
    /// The estimate's low end, bytes.
    pub low: u64,
    /// Its high end, bytes.
    pub high: u64,
    /// What the estimate is ([`SHARED_ESTIMATE_MODEL`]).
    pub model: String,
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

/// Reads a plan file (read only): a consumer plan's JSON.
pub fn read_plan(path: &Path) -> Result<ConsumerPlan> {
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

    /// The datasets the plans of `workload`'s runs scan besides the data
    /// root (none for a workload of plain lookups).
    pub(crate) fn workload_datasets(
        &self,
        workload: &Workload,
        keys: &dyn KeySetSource,
    ) -> Result<PlanDatasets> {
        let plans: Vec<ConsumerPlan> = workload_plans(workload, keys)?
            .into_iter()
            .map(|(_, p)| p)
            .collect();
        self.datasets_for(&[], &plans)
    }
}

impl PlanDatasets {
    /// Each dataset's catalog digest now, as a replay over it cites it.
    pub(crate) fn digests(&self) -> Result<Vec<Hash>> {
        self.held
            .iter()
            .map(|h| {
                Ok(
                    crate::fix::replayer(&h.engine, &*h.source, h.pinned.as_ref())?
                        .catalog()
                        .digest(),
                )
            })
            .collect()
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
pub(crate) fn sum_operators<'o>(
    blocks: impl IntoIterator<Item = &'o [OperatorCost]>,
) -> Vec<OperatorCost> {
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

/// The query a run of `plan` records: `asof` at `at` (in the unit of the
/// plan's `as_of` version columns, which must share one type) when the
/// plan has `as_of` nodes, else `locate`. An instant without `as_of`
/// nodes, or `as_of` nodes without an instant, is refused.
pub(crate) fn plan_query(
    replayer: &Replayer<'_>,
    plan: &ConsumerPlan,
    at: Option<&str>,
) -> Result<bellwether_replay::RunQuery> {
    let checked = replayer.check_plan(plan).map_err(Error::from)?;
    let columns = checked.asof_columns();
    match (columns.first(), at) {
        (None, None) => Ok(bellwether_replay::RunQuery::Locate),
        (None, Some(at)) => Err(Error::InvalidArgument {
            field: "at",
            reason: format!(
                "--at {at} is the instant of a plan's as_of nodes, and this plan has none; drop \
                 --at, or add an as_of node"
            ),
        }),
        (Some((d, c)), None) => Err(Error::InvalidArgument {
            field: "at",
            reason: format!(
                "the plan has an as_of node on {d}.{c}; give its instant with --at <instant> (RFC \
                 3339, a date, or an integer in {c}'s unit)"
            ),
        }),
        (Some((d, c)), Some(at)) => {
            let ty = |d: &str, c: &str| -> Result<arrow::datatypes::DataType> {
                let r = replayer.dataset(d).ok_or_else(|| Error::InvalidArgument {
                    field: "datasets",
                    reason: format!("dataset {d:?} is not bound"),
                })?;
                Ok(r.schema()
                    .field_with_name(c)
                    .map_err(|_| Error::KeySpec(format!("{c:?} is not a column of {d}")))?
                    .data_type()
                    .clone())
            };
            let first = ty(d, c)?;
            for (d2, c2) in &columns[1..] {
                if ty(d2, c2)? != first {
                    return Err(Error::InvalidArgument {
                        field: "at",
                        reason: format!(
                            "the plan's as_of nodes order versions by {d}.{c} and {d2}.{c2}, of \
                             different types, so one instant has no one unit; give every as_of \
                             node a version column of one type"
                        ),
                    });
                }
            }
            Ok(bellwether_replay::RunQuery::Asof {
                ts: crate::index::instant_in_unit("at", at, c, &first)?,
            })
        }
    }
}

/// Every plan `workload`'s runs name, in run order, each once, with the
/// runs priced through it.
fn runs_by_plan(workload: &Workload) -> Vec<(Hash, Vec<&bellwether_replay::RecordedRun>)> {
    let mut out: Vec<(Hash, Vec<&bellwether_replay::RecordedRun>)> = Vec::new();
    for run in workload.runs() {
        let Some(h) = run.plan else { continue };
        match out.iter_mut().find(|(p, _)| *p == h) {
            Some((_, runs)) => runs.push(run),
            None => out.push((h, vec![run])),
        }
    }
    out
}

/// `why --plan`'s operator tables: per plan the workload's runs name, each
/// node's cost summed over those runs under the current configuration and,
/// with `recommended`, with it added; each read occurrence and each shared
/// node's model estimate under the current configuration.
pub(crate) fn plan_reports(
    replayer: &Replayer<'_>,
    workload: &Workload,
    keys: &dyn KeySetSource,
    datasets: &[PlanDataset],
    recommended: Option<&KeySpec>,
) -> Result<Vec<PlanReport>> {
    let current = replayer.current_specs().to_vec();
    let proposed =
        recommended.map(|s| bellwether_replay::proposed_specs(replayer.current_specs(), s));
    let mut out = Vec::new();
    for (hash, runs) in runs_by_plan(workload) {
        let plan = load_plan(keys, &hash)?;
        let checked = replayer.check_plan(&plan).map_err(Error::from)?;
        let mut now: Vec<Vec<OperatorCost>> = Vec::new();
        let mut then: Vec<Vec<OperatorCost>> = Vec::new();
        let mut reads: BTreeMap<u32, PlanRead> = BTreeMap::new();
        let mut shared: BTreeMap<u32, SharedEstimate> = BTreeMap::new();
        for run in &runs {
            let rk = workload.run_keys(keys, run)?;
            let asof = run.query()?.asof_ts();
            let p = replayer
                .price_checked(&checked, &current, &run.run_id, &rk, asof)
                .map_err(Error::from)?;
            for r in &p.reads {
                let e = reads.entry(r.node).or_insert_with(|| PlanRead {
                    node: r.node,
                    dataset: r.dataset.clone(),
                    row_groups: 0,
                    bytes: 0,
                });
                e.row_groups = e.row_groups.saturating_add(r.row_groups.len() as u64);
                e.bytes = e.bytes.saturating_add(r.bytes);
            }
            for s in &p.shared {
                let e = shared.entry(s.node).or_insert_with(|| SharedEstimate {
                    node: s.node,
                    consumers: s.consumers,
                    low: 0,
                    high: 0,
                    model: SHARED_ESTIMATE_MODEL.to_string(),
                });
                e.low = e.low.saturating_add(s.estimate.0);
                e.high = e.high.saturating_add(s.estimate.1);
            }
            now.push(p.operators);
            if let Some(specs) = &proposed {
                let q = replayer
                    .price_checked(&checked, specs, &run.run_id, &rk, asof)
                    .map_err(Error::from)?;
                then.push(q.operators);
            }
        }
        let n = u32::try_from(plan.nodes.len()).unwrap_or(u32::MAX);
        out.push(PlanReport {
            plan: hash,
            runs: runs.len() as u64,
            datasets: datasets.to_vec(),
            consumers: (0..n).map(|v| checked.consumers(v)).collect(),
            current: sum_operators(now.iter().map(Vec::as_slice)),
            recommended: proposed
                .as_ref()
                .map(|_| sum_operators(then.iter().map(Vec::as_slice))),
            reads: reads.into_values().collect(),
            shared: shared.into_values().collect(),
        });
    }
    Ok(out)
}
