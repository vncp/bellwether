# ADR 024: Attribution by counterfactual replay is the backward pass

**Status:** accepted · **Date:** 2026-10-24

## Decision

For a candidate artifact, evolve computes for each plan node `v` the partial difference `Δ_v = c(a) − c(a ⊖ v)`, where `a ⊖ v` replaces `v` by its best known local alternative or ablates it, and for pairs the interaction `I_uv = c(a_uv) − c(a_u) − c(a_v) + c(a)`. Both are exact replays, recorded as `Attribution` and `Interaction` ledger records, and are the inputs to every proposer. Attribution is capped at the top-k nodes by cost rather than sampled.

**Amended 2026-10-25.** Attribution extends from plan nodes to **lineage edges**: `Δ_(s→t)` for each step boundary, computed by replaying consumer `t` under producer `s`'s alternative layout or materialization. On a **failed output certificate only**, run stepwise reconciliation along lineage to localize the breaking edit, and record the failing edge as a negative example proposers read. The top-k cap stays; k applies to nodes and edges jointly by cost.

## Rejected alternatives

- Asking the model to infer which operator is expensive from a trace. That is a guess where an exact number is available for the price of one replay.
- Shapley values over all subsets. Exponential; the pairwise interaction matrix captures what bundling decisions need.
- Sampling attribution for large plans. Reintroduces estimation; the cap on k keeps every reported number exact.
- **Amended 2026-10-25.** Stepwise reconciliation on every candidate. It would make intermediate equivalence a constraint, which forbids exactly the non-local rewrites §5.3 exists to certify.

## Why

A gradient is only useful if it is correct. Replay makes per-node sensitivities exact and cheap, which is the one place a data system has an advantage over every other domain where text-space optimization is applied.
