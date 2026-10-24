# ADR 006: Content-addressed, append-only evidence ledger

**Status:** accepted · **Date:** 2026-10-23

## Decision

Content-addressed, append-only evidence ledger.

## Rejected alternatives

SQLite state database; log files.

## Why

Receipts must be tamper-evident and re-derivable. Hash links make the trust boundary auditable.
