# ADR 008: Split-block bloom filters, xxhash64, over canonical typed bytes

**Status:** accepted · **Date:** 2026-10-23

## Decision

Split-block bloom filters, xxhash64, over canonical typed bytes.

## Rejected alternatives

Library bloom filters hashed over stringified values.

## Why

No stringification bugs: the filter hashes the same canonical bytes the key index orders by.

## Amendment 2026-10-24

The original text claimed sidecar filters are byte-compatible with Parquet's embedded filters. That is false as stated: Parquet embedded filters hash the column's *physical plain encoding* (for example little-endian INT64, raw BYTE_ARRAY bytes), not Bellwether's canonical encoding. The structure (split-block, xxhash64) is shared; the hash input is not.

Corrected decision: sidecar filters hash canonical bytes. When the engine reuses an embedded Parquet filter for a column chunk, it hashes the probe value with Parquet's physical encoding for that column's physical type, as the `parquet` crate does. Compatibility is achieved at probe time by encoding per filter origin, never by mixing inputs. A filter's origin is recorded with it so a probe cannot use the wrong encoding.
