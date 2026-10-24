# ADR 013: Source file changes: fingerprint drift tombstones old row-group ids, ids are never reused, segments are retained before GC

**Status:** accepted · **Date:** 2026-10-23

## Decision

Source file changes: fingerprint drift tombstones old row-group ids, ids are never reused, segments are retained before GC.

## Rejected alternatives

Path-only file identity; immediate deletion of unreferenced segments.

## Why

In-place rewrites and upstream compaction are detected instead of silently corrupting results. In-flight readers finish. `verify` reports drift.
