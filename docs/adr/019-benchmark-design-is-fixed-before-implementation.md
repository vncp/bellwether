# ADR 019: Benchmark design is fixed before implementation

**Status:** accepted · **Date:** 2026-10-23

## Decision

Benchmark design is fixed before implementation.

## Rejected alternatives

Benchmark chosen after the fact to fit results.

## Why

A synthetic versioned ledger with planted anti-patterns and recorded workloads. Baselines: raw scan, Parquet-native statistics and bloom filters, DuckDB, Apache DataFusion. Metrics: bytes read, latency versus corpus size, cost-estimation error, expert-fix recovery rate. The paper's claims are only credible if the evaluation was designed before the numbers existed.
