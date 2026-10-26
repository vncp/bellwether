# ADR 028: Storage version 2 and record version 3 are the frozen format

**Status:** accepted · **Date:** 2026-10-26

## Decision

Storage version 2 (`STORAGE_VERSION = 2`) and record version 3 (`RECORD_VERSION = 3`) are the format bellwether freezes at milestone M5. `ENCODING_VERSION` stays 1. After the freeze, a change to the bytes of a segment, a manifest or a record is a new version with a fixture, never an edit.

It lands in two pull requests, and the freeze is reached in two steps:

- **The first makes the record version 3 shapes final.** Every record field and `KeySpec` kind M5 holds is declared, with its serialized form pinned by a golden hash (`every_record_version_3_field_is_pinned` in `bellwether-types`). The second fills fields the first leaves empty; it adds no record field and changes no field's serialized form.
- **The second freezes the bytes of storage version 2, and that is M5.** It changes storage version 2 bytes the first does not write: a manifest column's `data_type` and the source `dialect`, per-key `[bounds]` in the exact value record, and the `bitemporal` layout. Until it lands, storage version 2 is not frozen. A binary from the first pull request refuses each of those shapes by name with `Error::UnsupportedShape` ("… storage version 2 segment/manifest shape not supported by this binary; upgrade bellwether to read this index", exit 4, MCP `unsupported_version`): never a serde error, and never "a newer storage version", because the version is the same.

Each pull request fills a named subset, and neither is a placeholder for the other.

**Landed by the first (WP14/B1):**

- **Per-key counts.** `counts: true` on an exact or temporal spec (`exact:col,counts`, `temporal:id@ts,counts`). The value record holds one LEB128 count per row group holding the key, as `docs/format.md` "Segments, storage version 2" describes. `count --keys` answers `count(*) where key in (...)` from the index with no data file read. Plans through a counted spec carry exact rows. Replay prices counts, and rule `R-counts-1` finds a count query that precedes a query over the same keys.
- **Checksums.** Every segment region a read touches has an XXH3-64 checksum: the footer over preamble, header and index; each filter, block and value record. A corrupt byte is refused, never decoded into a wrong count.
- **Manifest statistics.** `rowgroup_rows`, `rowgroup_bytes` and `column_bytes` per file, filled at build and on the first commit over a version 1 manifest. A plan opens no Parquet footer, and `locate --columns` prices a projection exactly.
- **Identity keys.** `KeySpec::identity()` holds every field of a spec and keys `segments[].key_spec` and `key_spec_completeness`, so `bloom:a@0.01` and `bloom:a@0.05` never share completeness. `name()` stays the display name and the one-spec-per-name slot.
- **Record version 3 shapes**, all serde-defaulted so a version 2 record reads unchanged:
  - `Exactness` (`Exact`, `Bounded`, `Measured`) and `Quantity` on `RunCost`, `rows` and `operators`;
  - `Replay.configuration` and `Replay.unserved`;
  - `ScanEntry.columns` and `ScanPlan.{projected_bytes, order}`;
  - `CertBody.{examples, uncomparable, totals, row_filter}`;
  - `Fanout.{sample_size, quantile_error}` and `Lint.fix`;
  - `ProposalBody::Key.sidecars`, and `RewriteShape.{before_plan, after_plan}` with `ConsumerPlan`;
  - the `SemiJoin`, `Join` and `WindowPartition` predicates.
  
  Ledger rule 7b reads `exactness`, and rule 9 passes a certificate only when `uncomparable == 0`.
- **Fixtures.** `fixtures/format-v1/` was built by the last version 1 binary. `fixtures/format-v0.1/` was built by this one, with counts; its status is pre-M5: shapes final, bytes frozen at the second pull request, which must still read it. CI replays every plan, count and record hash both of them pin (`crates/bellwether-cli/tests/format_compat.rs`).
- **The typed refusal of later shapes** (`Error::UnsupportedShape`), above, with a test per shape in the store (manifest), core (segment header) and CLI (exit 4, the message, no serde text).

**Filled by the second:**

- **Verify** fills `examples`, `uncomparable`, `totals` and `row_filter`.
- **Replay** fills `configuration`. It already sets `exactness`: `Exact` everywhere, and `Measured { model }` for a layout replay; the facade sets `Measured { model }` on every cost of a replay over a SQL table (ADR 027), whose runs no spec serves stay priced as a raw scan (`unserved` absent).
- **Profile** fills `sample_size`, `quantile_error` and `Lint.fix`, and the facade fills a key proposal's `sidecars`.
- **Plans:** `plan_ordered` fills `ScanPlan.order`, so two files can interleave.
- **Folded sidecars:**
  - per-key time bounds move into the exact value record (`[counts] | [bounds]`, with the all-null sentinel);
  - bitemporal versions get a `bitemporal` layout;
  - `KeySpec::Exact.bounds` and `KeySpec::Bitemporal` become declarable as key specs;
  - a bloom with bounds keeps its `bounds` sidecar.
- **Manifest:**
  - `schema.columns[].data_type` in core's `ColumnType` spelling;
  - `source.dialect` in place of the `csv;delimiter=…` encoding;
  - the widest decimal across files when a new index infers its schema.
- **A fixture** for the bytes it adds. It must still read `fixtures/format-v0.1/` unchanged.

## Rejected alternatives

- **Counts as a sidecar kind.** A count is read with the key's bitmap, for the same keys. A separate segment doubles the reads for the operator counts exist to make cheap.
- **Fixed-width counts.** Most counts are small. LEB128 in its shortest form keeps one spelling per value, so equal content still hashes to one segment id.
- **Keying the manifest by `name()`.** Specs that differ only in false-positive rate, counts or time column collided. A redefinition could be served by the old definition's segments.
- **One checksum over the whole segment.** Verifying it means reading the whole file, which breaks the bounded-read lookup (ADR 018).
- **Statistics in a separate object beside the manifest.** That is a third object to commit atomically. The arrays cost about 8 bytes per (row group, column) in the manifest. Past about 10^5 row groups × 50 columns, externalized statistics are a storage version 3 question.
- **Upgrading on open, or on a no-op build.** A read never writes, and an `index` run with nothing to do commits nothing. A version 1 index stays readable by a version 1 binary until a build has real work. That build upgrades it and says so.
- **Freezing in one pull request.** The diff would be too large to review against the acceptance bar. The record shapes become final first, so the four index kinds in flight can rebase onto them; the bytes freeze with the second.
- **Calling storage version 2 frozen at the first pull request.** The second changes version 2 bytes (the manifest's `data_type` and `dialect`, `[bounds]`, the `bitemporal` layout) without a version bump, so a first-release binary given a second-release index would fail with a serde error or call the index a newer version, which it is not. Reading those shapes in the first pull request was rejected too: it would land half of the second's format with no writer to test it against. The first refuses them by name instead.

## Why

ADR 026 prices the relational subset exactly from statistics and counted indexes. That needs per-key counts and per-column bytes on disk. Both change bytes, and ADR 017 says a byte change is a version bump. Making the bump the freeze means bellwether changes its format once, not twice.

## Consequences

- A version 2 binary reads version 1 manifests (keys rewritten from names to identities) and version 1 segments (no checksums, no counts). It writes only version 2 segments. A commit stamps a version 1 manifest `storage_version: 2` only when every file carries the row-group statistics (a build with work fills them); a commit that fills nothing (compaction, garbage collection, removing a spec) writes it back at version 1, in names.
- A version 1 binary refuses a version 2 index with `Error::UnsupportedVersion`. The upgrade is one-way, and `bellwether index` names it.
- A first-release version 2 binary refuses a second-release version 2 shape with `Error::UnsupportedShape`, naming the shape and saying to upgrade.
- `docs/format.md` documents the version 2 bytes: the full segment layout with a worked segment, conformance vectors, the minimal manifest, the sidecar proposal vectors, and "Reading storage version 1".
- `docs/contract-v3.md` marks each entry this ADR closes.
