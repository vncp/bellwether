# ADR 003: Immutable segments, a manifest, and conditional-PUT commits

**Status:** accepted · **Date:** 2026-10-23

## Decision

Immutable segments, a manifest, and conditional-PUT commits.

## Rejected alternatives

A mutable index table maintained with MERGE and a staging table; file locks with stale-lock healing.

## Why

Snapshot isolation without locks. Parallel build is a consequence, not a feature. No delete-then-append window.
