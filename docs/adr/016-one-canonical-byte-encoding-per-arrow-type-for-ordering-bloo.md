# ADR 016: One canonical byte encoding per Arrow type for ordering, bloom hashing, and reconcile row hashing

**Status:** accepted · **Date:** 2026-10-23

## Decision

One canonical byte encoding per Arrow type for ordering, bloom hashing, and reconcile row hashing.

## Rejected alternatives

toString hashing; engine-specific casts.

## Why

Memcomparable ordering, xxhash64 over the same bytes for blooms, and row hashing with fixed column order, explicit null marker, and decimals normalized to declared scale. One table removes a known class of timestamp-formatting and binary-identity hashing defects and makes certificates comparable across runs and machines.
