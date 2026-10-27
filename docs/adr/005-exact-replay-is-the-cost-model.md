# ADR 005: Exact replay is the cost model

**Status:** accepted · **Date:** 2026-10-23 · **Amended:** 2026-10-27

## Decision

Exact replay is the cost model.

## Rejected alternatives

Optimizer-style estimation; LLM-estimated cost.

## Why

A lake exposes its files, so the cost of a hypothetical index can be computed exactly from statistics. Zero estimation error is the thesis.

## Amendment 2026-10-27 (WP15)

The cost model is exact per operator, and bounded where the operator table says so (ADR 026, the README's "Pricing a whole query"): a node's bytes and output rows are each `exact`, `bounded` (an interval proved to hold the true value, never a point estimate), or `measured` (naming the model, for a SQL table). Nothing is estimated: where a statistic cannot give the exact value, the replay gives the tightest interval the index and footers prove.
