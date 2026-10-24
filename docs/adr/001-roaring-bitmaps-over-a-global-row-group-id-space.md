# ADR 001: Roaring bitmaps over a global row-group id space

**Status:** accepted · **Date:** 2026-10-23

## Decision

Roaring bitmaps over a global row-group id space.

## Rejected alternatives

A finite-state transducer keyed by value and row-group id, which repeats each value once per row group; per-file arrays of distinct values, which must be exploded on every lookup.

## Why

One bitmap per value instead of the value repeated per row group. AND and OR in microseconds. Composite pruning via intersection at row-group granularity.
