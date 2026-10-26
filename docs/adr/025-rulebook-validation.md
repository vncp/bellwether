# ADR 025: A rule is promoted only by held-out replay

**Status:** accepted · **Date:** 2026-10-24

## Decision

The rulebook is distilled from winning mutations by a language model into **parameterized templates**: detector, action, invariant, an **applicability predicate** over evidence shape, and the parameters to re-fit on a new workload. No rule is accepted on the model's word and no rule carries a workload constant. A rule enters the rulebook only if applying it to held-out runs (excluded from the search) reproduces a gain under exact replay and passes the equivalence gate. Rules carry a confidence derived from that validation, hash links to the evidence and receipts that produced them, and the workload coverage of their certificates. A rule whose evidence has drifted is flagged stale.

**Amended 2026-10-25.** The first sentence above replaces the original, which read: "The rulebook is distilled from winning mutations by a language model, but no rule is accepted on the model's word." The held-out set is **temporal**, the newest runs by recorded time, not a random slice. Each rule carries `helpful` and `harmful` counters incremented by later searches that applied it (gain or regression under replay); a rule whose `harmful` exceeds `helpful` over a window is retired to the archive with its evidence. Buckets (per detector family) have a cap; a full bucket admits a new rule only as a merge or revision of an existing one, with near-duplicates routed by signature. A rule flagged stale by drift (ADR 025, existing) is not deleted; it re-earns promotion on the new runs or is retired.

## Rejected alternatives

- Accepting distilled rules directly. Accumulates folklore and overfits to the search workload.
- No rulebook; keep only the frontier. Loses the transferable artifact, which is the part that generalizes to new queries and the part a human reads.
- **Amended 2026-10-25.** Counting a rule verified when it was promoted. Promotion is one holdout; counters are what the rule did afterward.

## Why

This is SkillOpt's validation gate applied to rules instead of skills. It is also the paper's transfer experiment: rules learned on one workload, applied to held-out templates, measured by replay.
