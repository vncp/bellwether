# ADR 031: Scoped proposers and a frozen outer loop

**Status:** accepted · **Date:** 2026-10-25

## Decision

The structural proposer is two prompts with different evidence slices. The *invariant* proposer owns identities, operator order, step fusion and fission, and materialization: changes that are valid on any workload with the same plan shape. The *workload* proposer owns this workload's keys, layouts, cadence, and knob starting points: changes justified by this ledger's fanout, selectivity, and interaction evidence. Both read the attribution record; neither reads a score. After each search, a **reflection step** writes an `Experience` record per accepted and per rejected mutation: proposer scope, evidence shape at proposal time (fanout band, selectivity band, interaction sign), the mutation class, and the exact gain per replay and per model call. Proposers read matching experience records on later searches. The **outer loop is not editable by anything in the loop**: scorer, verifier, gate, Pareto rule, anchor set, and splits are fixed inputs; a proposal touching them is rejected by lint and recorded as an attempt.

## Rejected alternatives

- One generic proposer: measured worse in the closest analogue by 2.9–3.7 points, because it conflates "this rewrite is always valid" with "this key is right here."
- A self-editing meta-level: outruns audit; the measured benefit of experience comes from reading it, not from rewriting the improver.

## Why

Gain per call is measured exactly by replay, so the experience record is honest in a way it cannot be for text-only optimizers; the boundary keeps ADR 009 intact.

## Scope

Ships in stage 2 (`bellwether evolve`, WP13b) behind `--llm`, and only when the stage-1 trigger in `docs/evolve.md` §3 fires (ADR 032). The closest analogue is Harness²'s split of the editor into an invariant scope and a domain scope, and its experience-library ablation; see `docs/evolve.md` §2.1 for the sources.
