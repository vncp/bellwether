# ADR 021: Reflective workload optimization with an exact loss and an equivalence gate

**Status:** accepted · **Date:** 2026-10-24

## Decision

Bellwether gains an optimizer (`bellwether evolve`, crate `bellwether-evolve`) that improves a target artifact against a recorded workload by reflective, population-based search in the style of GEPA and SkillOpt, with three substitutions that make it sound for data systems:

- **The loss is exact, not a benchmark score.** Each candidate is scored on every recorded run by replay (bytes, row groups, files; or, through a pluggable `Scorer`, measured execution cost from an external engine). No estimates.
- **The constraint is hard.** A candidate enters the population only if the `Verifier` passes (reconciliation certificate for lakes; result-set hash equality over sampled parameters, optionally a SQL equivalence prover, for databases). Faster-but-wrong is discarded, never ranked.
- **Selection is Pareto over runs, not mean over runs.** A candidate survives if it is best on at least one recorded run, so a rewrite that wins at p50 and loses at the p95 batch does not eliminate the one that protects the tail.

The target artifact is text or configuration: an index key set, a file layout, a declarative pipeline step, a SQL or KQL query. The proposer is an LLM given the parent's trace (plan, per-operator bytes, fanout evidence, the funnel) and asked for one mutation. Every proposal, score, certificate, and the LLM's exact input and output are ledger records, so a run is replayable and auditable, and the trust boundary of ADR 009 holds unchanged: the model proposes, the engine scores and gates.

The transferable output is twofold: the best candidate with its receipt, and a **rulebook**, a text artifact distilled from winning mutations with links to the evidence that justified each rule. The rulebook is what an agent reads on the next `fix`, and it is the learned analogue of a hand-written optimization catalog.

**Amended 2026-10-25.** The loop ships in three stages: `advise` (deterministic proposers only), `evolve` (adds the scoped structural proposer and composition), and rulebook promotion. Each stage is independently useful and gated by a measured trigger recorded in `docs/evolve.md` §3. After candidates are generated, a deterministic **composer** bundles structural, layout, and knob changes using the interaction matrix: pairs with `|I_uv|` above a threshold travel together; additive pairs are proposed separately. Admission uses **preserve-and-extend**: a child enters the population only if its net gain over the parent across runs is positive *and* the sum of per-run regressions is within a declared budget δ, with δ small and nonzero. A held-out **anchor set** of runs per template (the p95 and max key-set sizes) is replayed on every admission and may not regress beyond δ. Budget exhaustion remains a recorded terminal state.

## Rejected alternatives

- Single-shot LLM rewriting with a verifier (GenRewrite, LITHE, QUITE): no search over a workload, no tail protection, no persistent learned artifact.
- Reinforcement learning or gradient-based tuning: the artifacts are text and configuration, there is no differentiable path, and sample efficiency of reflective search on text has been shown to exceed RL on prompt-like artifacts.
- Mean-cost selection: collapses the batch-size distribution that the profiler measures at p50, p95, and max.
- **Amended 2026-10-25.** Pareto update alone. It protects the tail inside the search workload but says nothing about runs the search never saw.

## Why

Query optimization by LLM is only trustworthy if the score cannot be gamed and the constraint cannot be waived. Lakes expose their files, so the score is exact; reconciliation makes the constraint checkable without a prover. With both in place, the search loop from text-skill optimization applies directly, and the thing it learns is the estate's own rulebook.

## Scope

Built after M3 (receipts exist). Sources of cost beyond replay, including database engines, arrive through the `Scorer` and `Verifier` traits, which is what makes the optimizer useful outside lakes without changing the engine.
