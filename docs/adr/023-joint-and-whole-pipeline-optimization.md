# ADR 023: Optimize sets and pipelines, not calls

**Status:** accepted · **Date:** 2026-10-24

## Decision

The unit of optimization is never a single call. (1) Plans are mutated as DAGs and scored whole, so plan-internal interactions are captured; a proposal may bundle edits that the interaction matrix shows do not compose additively. (2) Physical design is a set-valued configuration (key specs, layout, knobs) scored against every recorded run with subsumption modeled and maintenance cost as a separate objective; search is greedy construction plus local search with exact scoring. (3) Pipelines are optimized as DAGs of steps with lineage, cross-step mutations are allowed, and equivalence is certified at the pipeline output over the touched keys, so non-local rewrites that change intermediate outputs are admissible.

**Amended 2026-10-25.** **Layout** (sort order, partitioning, row-group or chunk target, per intermediate) and **Cadence** (batch window, incremental vs full recompute via `latest`/`asof`) are first-class artifacts in the search space alongside plan, design, and step boundaries. Fanout is a property of layout, and layout is set by the step that writes an intermediate, so full-pipeline optimization is layout co-design: the interaction matrix carries layout × downstream-plan terms, and the composer may bundle an upstream layout change with the downstream plan change that only pays off given it. Materialization placement is searched as a set over shared intermediates scored by replay, with maintenance cost as its own objective.

## Rejected alternatives

- Per-query optimization with per-query equivalence. Misses shared indexes, conflicting optima across runs, and every rewrite that crosses a step boundary.
- Scalarizing the workload cost. Hides tail runs; see ADR 021's Pareto rule.
- Requiring local equivalence per step. No prover can certify a push-down across steps; data-level equivalence at the output can.
- **Amended 2026-10-25.** Treating layout as fixed input. The ClickBench measurement (an exact key pruning 1 row group of 28 on data not laid out by that key) is the counterexample.

## Why

The expensive problems in real pipelines are interactions: the composite key that only pays off once the prefilter moves, the lookback bound that is safe only because a downstream dedup exists. A method that cannot express those is a per-query rewriter with a search loop attached.
