# ADR 007: Parquet-native; no new file format

**Status:** accepted · **Date:** 2026-10-23

## Decision

Parquet-native; no new file format.

## Rejected alternatives

Custom columnar container.

## Why

Adoption. Parquet footers and row groups already carry most of what the engine needs.
