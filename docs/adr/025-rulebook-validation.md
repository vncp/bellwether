# ADR 025: A rule is promoted only by held-out replay

**Status:** accepted · **Date:** 2026-10-24

## Decision

The rulebook is distilled from winning mutations by a language model, but no rule is accepted on the model's word. A rule enters the rulebook only if applying it to held-out runs (excluded from the search) reproduces a gain under exact replay and passes the equivalence gate. Rules carry a confidence derived from that validation, hash links to the evidence and receipts that produced them, and the workload coverage of their certificates. A rule whose evidence has drifted is flagged stale.

## Rejected alternatives

- Accepting distilled rules directly. Accumulates folklore and overfits to the search workload.
- No rulebook; keep only the frontier. Loses the transferable artifact, which is the part that generalizes to new queries and the part a human reads.

## Why

This is SkillOpt's validation gate applied to rules instead of skills. It is also the paper's transfer experiment: rules learned on one workload, applied to held-out templates, measured by replay.
