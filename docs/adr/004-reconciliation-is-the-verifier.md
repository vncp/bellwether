# ADR 004: Reconciliation is the verifier

**Status:** accepted · **Date:** 2026-10-23

## Decision

Reconciliation is the verifier.

## Rejected alternatives

SQL equivalence provers (QED, SQLSolver, EquiTrace).

## Why

Provers do not accept Spark plans, KQL, or Scala and cover restricted SQL only. Data-level equivalence over the keys a run touched generalizes, and the index makes it cheap.
