# ADR 009: The agent proposes; only the engine scores, verifies, and applies

**Status:** accepted · **Date:** 2026-10-23

## Decision

The agent proposes; only the engine scores, verifies, and applies.

## Rejected alternatives

Agent scores its own proposals; engine calls the model.

## Why

This is the trust boundary. The model is a hypothesis generator with no authority. The engine never calls the model; the model calls the engine.

## Amendment 2026-10-24: evolve and the proposer

The sentence "the engine never calls the model" was too broad once `evolve` (ADR 021) existed: the search loop does invoke a language model, as one of its proposers. The precise rule is: the engine never consults a model for a score, a verification outcome, or an apply decision. A proposer, model or otherwise, is invoked only to produce a `Proposal` through the same path an external agent uses, its prompt and completion are ledger records, and nothing it returns is trusted. The direction of trust is unchanged; what changed is that the engine may host an untrusted proposer rather than only wait for one.
