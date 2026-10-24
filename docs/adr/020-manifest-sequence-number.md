# ADR 020: The manifest carries a monotonic sequence number

**Status:** accepted · **Date:** 2026-10-24

## Decision

Every committed manifest carries `sequence: u64`, incremented by exactly one per successful commit. `key_spec_completeness` maps a key-spec name to the `sequence` at which that spec became complete over every tracked row group. Replay, Certificate provenance, and Receipt `manifest_before` and `manifest_after` reference manifests by `sequence` rendered as a string, not by the store's version token.

## Rejected alternatives

Using the store's version token (etag, generation) as the manifest identity. A manifest cannot contain its own token, tokens have no order, and they differ per backend, so completeness and before/after relations could not be expressed or compared.

## Why

The commit protocol (ADR 003, 015) already serializes commits through a conditional PUT, so a counter is trivially consistent: the writer reads `sequence = n`, commits `n + 1`, and a conflict means someone else committed `n + 1` first and the rebase re-reads. A small integer is also what humans and agents can compare in a receipt.
