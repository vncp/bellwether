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

## Amendment 2026-10-26: additive label fields in ledger records

A ledger record may gain an optional **label field** without a `RECORD_VERSION` bump when all of these hold:

- it is omitted when absent, so every record written before it existed keeps its bytes and its hash;
- it only names how values already in the record were obtained; it never changes the meaning, unit, or validity of an existing field, so an older reader that drops it reads every other field correctly;
- the ledger rules on its values (an unknown value is refused, never stored as "present").

A field that would change how an existing field is read still requires a `RECORD_VERSION` bump, as `inputs` and `model` did in contract v2.1.

The first such field is `Replay.unserved` (`"parquet_stats"` or absent): it says that runs no key spec serves were priced as the files' own min/max statistics read them, while `current` and `proposed` remain exact byte sums of the row groups each plan reads. An older build reading such a Replay sees the same costs and lacks only the label. The record-version bump to 3 and the frozen record shapes arrive with storage version 2 (PR #23), which carries `unserved` unchanged.

## Amendment 2026-10-26 (B8): a parameter whose absence means something else is a version bump

The label-field rule above does not cover a field an older reader would misread by dropping it. B8's tombstone is one: a `KeySpec::Temporal` or `KeySpec::Bitemporal` with a `tombstone`, `Predicate::Live`, and `RecordedRun.tombstone` each change what the record or run means (another key, another row filter, another query). Dropped, the record still parses and reads as something else: the M5 binary (e1bd168) read a Proposal of `temporal:id@ts,tombstone=void` as `temporal:id@ts` and passed `fix --verify-only` on it.

So each side follows its own rule:

- **Storage** keeps `STORAGE_VERSION` 2: the manifest never holds the parameter in `key_specs` (the store refuses a key spec that carries one). The tombstone is an additive sidecar kind (ADR 028), which an older binary preserves, never plans with, and never misreads.
- **Records** bump `RECORD_VERSION` to 4 (ADR 028, "Records after the freeze"). A version 3 reader refuses a version 4 record with `Error::UnsupportedVersion` before parsing it, whatever the record holds, so it never sees a record with the parameter dropped. Records of versions 1 to 3 are read unchanged and never rewritten.
- **Workloads** are not ledger records and carry no record version; their stored bytes must be the canonical JSON of the manifest a reader parses, so a reader that would drop `tombstone` refuses the workload ("not stored in canonical form").

Every rule is held against the M5 binary by `crates/bellwether-cli/tests/tombstone_old_binary.rs`.
