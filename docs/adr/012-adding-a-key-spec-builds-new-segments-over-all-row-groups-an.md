# ADR 012: Adding a key spec builds new segments over all row groups and records completeness in the manifest

**Status:** accepted · **Date:** 2026-10-23

## Decision

Adding a key spec builds new segments over all row groups and records completeness in the manifest.

## Rejected alternatives

Lazy backfill with partial answers; rebuild from scratch.

## Why

A query on an incomplete spec fails, never returns partial. A backfill that deletes old rows before writing their replacements holds a crash window in which a file is indexed but incomplete; completeness as a manifest fact removes it.
