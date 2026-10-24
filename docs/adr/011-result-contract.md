# ADR 011: Result contract: a ScanPlan is a superset with a residual predicate

**Status:** accepted · **Date:** 2026-10-23

## Decision

Result contract: a ScanPlan is a superset with a residual predicate.

## Rejected alternatives

Promise row-exact output from pruning alone; leave outer-join semantics undefined or reject outer joins outright.

## Why

Every row group holding a queried key is included; extra rows may be present; applying the residual makes the result row-exact. Every join type is then well-defined.
