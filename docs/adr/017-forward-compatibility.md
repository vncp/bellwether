# ADR 017: Forward compatibility: unknown fields are preserved on rewrite; a newer format version is refused with a typed error; the engine never downgrades in place

**Status:** accepted · **Date:** 2026-10-23

## Decision

Forward compatibility: unknown fields are preserved on rewrite; a newer format version is refused with a typed error; the engine never downgrades in place.

## Rejected alternatives

Strict schemas; best-effort reading of newer formats.

## Why

The same rule Delta Lake and Apache Iceberg readers enforce for newer protocol versions, adopted from the first commit rather than retrofitted.

## Amendment 2026-10-24: scope of unknown-field preservation

Unknown fields are preserved, via flattened extra maps, in every struct the manifest owns: the manifest itself, file entries, segment entries, row-group ranges, tombstones, and GC claims. They are not preserved inside structs owned by `bellwether-types` (`FileFingerprint`, `KeySpec`, ledger records), because adding extra maps there would change every constructor in every crate.

The rule for those types is versioning instead of preservation: a new `KeySpec` kind or a new `FileFingerprint` field requires a `storage_version` bump, and a reader that meets an unknown kind refuses with `Error::UnsupportedVersion`, never with a serialization error. Ledger records are never rewritten, so `get_raw` preserves their bytes regardless.
