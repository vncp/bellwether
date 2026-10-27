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

## Amendment 2026-10-27: stage 1 reduced, stages 2 and 3 deferred

Measured since this ADR was written: planning with the index and reading only the planned row groups lost to both a full scan and DuckDB in all 42 lookups of `docs/measured/2026-10-26-performance.md`, though it read 241,358 of 31,932,514 bytes at 10 keys; plan time, one store round trip per indexed value, is the cost. An optimizer whose loss is bytes read therefore does not yet optimize time, and until it does more search buys nothing a user can feel. And the structural rewrites stage 2 exists for need a recorded multi-step pipeline with lineage, which no workload Bellwether has seen supplies; its input today is a hand-written `ConsumerPlan`.

Decision:

- **WP13a `advise` is reduced** to workload-level physical design: a set of key specs chosen against every recorded run, with subsumption and index bytes and build time as the maintenance cost; exact descent on the numeric knobs replay already prices (bloom false-positive rate, the key-count gates); Pareto over runs × objectives with anchors and δ; a receipt for every admitted change; `bellwether advise` and MCP `score.advise`. It starts after WP22b's lookup fix, so that fewer bytes read is also less time.
- **Deferred out of 13a:** layout and materialization search, the `Layout` and `Cadence` artifacts, lineage-edge attribution and bisect-on-failed-certificate, and plan-node counterfactual attribution over `ConsumerPlan` (the backward pass's only consumer is a 13b proposer). WP15 accordingly prices plans and rewrites but does not carry `replace_node`/`interaction` over plan nodes or layout pricing; the key-spec counterfactuals in `bellwether-replay` stay.
- **WP13b `evolve --llm` and WP13c rulebook v2 are deferred, not scheduled.** An external coding agent already proposes keys and rewrites over MCP with the engine scoring, verifying, and applying (ADR 009); stage 2 would move that model call inside the engine. Reopening either needs a new amendment that names a recorded multi-step pipeline workload, shows reduced `advise` leaving a structural anti-pattern on it, and writes the stage-2 budget into `docs/evolve.md` §3.1. The design in `docs/evolve.md` and ADRs 021 to 025 and 031 stands as the plan for that case.
