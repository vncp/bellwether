# ADR 018: Memory is bounded per operation

**Status:** accepted · **Date:** 2026-10-23

## Decision

Memory is bounded per operation.

## Rejected alternatives

Collect-to-driver patterns that materialize a full filename set in one process.

## Why

Build streams one row group per worker. Lookup holds one block index plus the bitmaps for the keys asked. Replay holds one run's key set. The bound is stated and benchmarked.
