# ADR 014: Nulls are not indexed; null timestamps rank last and never win latest

**Status:** accepted · **Date:** 2026-10-23

## Decision

Nulls are not indexed; null timestamps rank last and never win latest.

## Rejected alternatives

Index nulls as a distinct value group.

## Why

Avoids a class of null-group counting defects at large-index boundaries. Semantics match SQL three-valued logic.

## Amendment 2026-10-24: nulls inside composite keys

"Nulls are not indexed" is stated per key tuple, not per column. A tuple is indexed when its leading column is non-null; a null in a trailing column is encoded with the null marker so the tuple remains prefix-seekable by its leading columns. A probe whose tuple contains a null matches nothing, matching SQL three-valued logic. Consequently a composite key fully replaces its leading single-column key for prefix lookups, including rows whose trailing columns are null, and the earlier concern that it would not is resolved by encoding rather than by exclusion.
