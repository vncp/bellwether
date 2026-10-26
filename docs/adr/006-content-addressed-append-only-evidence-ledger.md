# ADR 006: Content-addressed, append-only evidence ledger

**Status:** accepted · **Date:** 2026-10-23

## Decision

Content-addressed, append-only evidence ledger.

## Rejected alternatives

SQLite state database; log files.

## Why

Receipts must be tamper-evident and re-derivable. Hash links make the trust boundary auditable.

## Amendment 2026-10-25: signatures beside records (ADR 029)

The ledger also holds detached ed25519 signatures over receipts and certificates, one object per key at `ledger/<record hash>.sig.<key id>.json`. They are not records: the record bytes, their hashes, and `RECORD_VERSION` are unchanged, and record readers skip them. Content addressing still only detects alteration; a signature from a key the reader trusts, kept outside the store, is what makes a planted chain refusable.
