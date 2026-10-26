# ADR 022: What "gradient descent" means for evolve

**Status:** accepted · **Date:** 2026-10-24

## Decision

Evolve uses three proposers against one exact scorer and one gate. (1) For discrete structure (operator order, keys, layouts) the proposer is a language model given a critique grounded in exact per-node attribution; this is a textual gradient in the sense of ProTeGi and TextGrad, with the "backward pass" replaced by counterfactual partial differences (ADR 024). (2) For numeric knobs (false-positive rate, key-count gates, lookbacks, chunk and segment sizes, partitions) the proposer is exact finite-difference descent: coordinate descent and integer line search on a deterministic cost. (3) Rule-driven enumeration seeds the population from the catalog's detectors. A learned differentiable surrogate is admissible only as a proposer, never as a scorer, and is deferred.

**Amended 2026-10-25.** Structural proposals pass a **generalization lint** before any replay: no workload constants (column values, key literals, file names) in rule text; every proposed identity carries an applicability condition over evidence shape (fanout profile, selectivity band, interaction sign); no proposal may name a run id. A proposal failing lint is returned once with the message, then discarded. The learned-surrogate boundary in §4.5 is unchanged and now cites the result that self-evolution without execution-based verification scored below an unmodified baseline.

## Rejected alternatives

- Calling the loop gradient descent without qualification. Artifacts are discrete; there is no differentiable path from a plan to its cost.
- Reinforcement learning over rewrites. Sample-inefficient on text-shaped artifacts and dependent on a reward model; reflective search has been shown to exceed it on comparable problems.
- A surrogate cost model as the scorer. It is an estimator, and the thesis is that the loss is exact.

## Why

Honesty about the mechanism is what makes the paper defensible. Descent is real for knobs because the cost is deterministic and finite differences are exact. For structure, the gradient is textual and grounded in exact attribution rather than inferred, which is the measurable difference from prior LLM rewriters.
