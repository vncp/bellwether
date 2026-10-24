# ADR 015: Writer conflicts resolve by rebase

**Status:** accepted · **Date:** 2026-10-23

## Decision

Writer conflicts resolve by rebase.

## Rejected alternatives

File locks with stale-lock healing; single writer only.

## Why

A losing CAS on the manifest re-reads, unions its new segments with the winner's, and retries. Segments are additive so rebase is a union. Readers never block. No TOCTOU, no lock-timeout tuning.
