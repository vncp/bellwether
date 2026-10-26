# ADR 032: Staged delivery with a measured trigger

**Status:** accepted · **Date:** 2026-10-25

## Decision

`bellwether advise` ships first and alone: rule-driven enumeration, exact knob descent, layout and materialization search, Pareto with anchors, receipts. No language model. `bellwether evolve` adds ADR 031's proposers and the composer behind a flag, and ships only if `advise` leaves planted anti-patterns unrecovered on the synthetic ledger and at least one real workload. Rulebook promotion ships only after `evolve` has produced a rule that reproduces its gain on a temporally held-out template. Each trigger is a number in `docs/evolve.md` §3 and a row in the ledger.

## Rejected alternatives

- Shipping the population search first. It is the research artifact, not the product; the product is the exact what-if with a receipt, and most of its value needs no model.

## Why

Across the harness literature, the deterministic components (scripts, rule-based context management) were the robust wins and the model-driven edits were the ones that needed gates. The same ordering holds here, with the added advantage that the deterministic stage is already sound.

## Scope

Work packages WP13a (`advise`), WP13b (`evolve`), and WP13c (rulebook v2) replace the single WP13; each opens the next only when its trigger in `docs/evolve.md` §3 is met. The kill criterion there stops stages 2 and 3 if stage 1 already captures the frontier.
