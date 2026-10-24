# ADR 002: Memcomparable composite key encoding

**Status:** accepted · **Date:** 2026-10-23

## Decision

Memcomparable composite key encoding.

## Rejected alternatives

Concatenated strings; separate per-column indexes intersected by file.

## Why

Prefix seek by leading column for free. Typed ordering preserved. No string canonicalization bugs.
