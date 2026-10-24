# ADR 023: Optimize sets and pipelines, not calls

**Status:** accepted · **Date:** 2026-10-24

## Decision

The unit of optimization is never a single call. (1) Plans are mutated as DAGs and scored whole, so plan-internal interactions are captured; a proposal may bundle edits that the interaction matrix shows do not compose additively. (2) Physical design is a set-valued configuration (key specs, layout, knobs) scored against every recorded run with subsumption modeled and maintenance cost as a separate objective; search is greedy construction plus local search with exact scoring. (3) Pipelines are optimized as DAGs of steps with lineage, cross-step mutations are allowed, and equivalence is certified at the pipeline output over the touched keys, so non-local rewrites that change intermediate outputs are admissible.

## Rejected alternatives

- Per-query optimization with per-query equivalence. Misses shared indexes, conflicting optima across runs, and every rewrite that crosses a step boundary.
- Scalarizing the workload cost. Hides tail runs; see ADR 021's Pareto rule.
- Requiring local equivalence per step. No prover can certify a push-down across steps; data-level equivalence at the output can.

## Why

The expensive problems in real pipelines are interactions: the composite key that only pays off once the prefilter moves, the lookback bound that is safe only because a downstream dedup exists. A method that cannot express those is a per-query rewriter with a search loop attached.
