# ADR 028: Storage version 2 and record version 3 are the frozen format

**Status:** accepted; **M5 declared** (the second pull request landed: storage version 2 and record version 3 are frozen); amended by B8 (record version 4, storage version 2 unchanged; "Consequences") · **Date:** 2026-10-26

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
- **Fixtures.** `fixtures/format-v1/` was built by the last version 1 binary. The fixture this pull request built (pre-M5: shapes final, bytes frozen at the second pull request) is `fixtures/format-v0.1-pre/` since the second pull request moved it there byte for byte. CI replays every plan, count and record hash it pins (`crates/bellwether-cli/tests/format_compat.rs`).
- **The typed refusal of later shapes** (`Error::UnsupportedShape`), above, with a test per shape in the store (manifest), core (segment header) and CLI (exit 4, the message, no serde text).

**Landed by the second (WP14/B1 PR 2), which declares M5:**

- **Verify** fills `examples`, `uncomparable`, `totals` and `row_filter`; an uncomparable key no longer counts in `mismatched`.
- **Replay** fills `configuration`. It already sets `exactness`: `Exact` everywhere, and `Measured { model }` for a layout replay; the facade sets `Measured { model }` on every cost of a replay over a SQL table (ADR 027), whose runs no spec serves stay priced as a raw scan (`unserved` absent). It prices a proposal's `sidecars` with its spec, and a bitemporal run (`why --valid-by/--known-by`, MCP `score.why` `valid_by`/`known_by`) against a `temporal2` candidate.
- **Profile** fills `sample_size`, `quantile_error` and `Lint.fix`, and the facade fills a key proposal's `sidecars`.
- **Plans:** `plan_ordered` fills `ScanPlan.order` when no order of whole files is a time order, so two files can interleave; when one is, `order` stays absent and the pinned plans of the earlier fixtures replay unchanged.
- **Folded sidecars:**
  - per-key time bounds move into the exact value record (`[counts] | [bounds]`, with the all-null sentinel `(i64::MAX, i64::MIN)`, which overlaps no window);
  - bitemporal versions get a `bitemporal` layout (`bitmap | count | (valid, known, rg)` triples);
  - `KeySpec::Exact.bounds` and `KeySpec::Bitemporal` are declarable as key specs, and `propose`/`fix` build, verify and receipt them (with any `sidecars`) end to end on CLI and MCP;
  - a bloom with bounds keeps them in its `bounds` segment, a storage version 2 segment in the `keyed` layout under the bloom spec whose records are `bitmap | bounds`, each checksummed, listed with `kind: "bounds"` under the bloom's identity (round 1 of the review moved it out of the unchecksummed version 1 sidecar layout before the freeze, since every region a read touches has a checksum);
  - an index written before M5 reads and plans as written; its first build with work folds its `bounds` and `temporal2` sidecars into key specs, rebuilds them, drops `schema.sidecars` entries of those kinds, and reports each fold (`sidecars_folded`). A sidecar no key spec can take (its spec's slot is held by one on another time column) is never discarded: it stays a declared sidecar, is built and still plans, and is reported (`kept_legacy`). Folding on read was rejected: a read never writes, and every pinned output of the earlier fixtures stays byte-identical.
- **Manifest:**
  - `schema.columns[].data_type` in core's `ColumnType` spelling (`{"arrow": …}` for a nested or dictionary type);
  - `source.dialect` in place of the `csv;delimiter=…` encoding;
  - the widest decimal across files when a new index infers its schema.
- **`Error::UnsupportedShape` is removed**: no shape of storage version 2 is unsupported by this binary.
- **Deferred to WP15: the relational predicates** (contract v3 entry 23). `Predicate::{SemiJoin, Join, WindowPartition}` are frozen in their record version 3 shapes, and the verifier refuses them. WP15 may only **add behaviour** to them (pricing and planning by relational replay); it adds no field, variant or serialized form to a record. A WP15 need that a pinned shape cannot carry is a new record version with a fixture, not an edit.
- **Fixture.** `fixtures/format-v0.1/` is rebuilt at M5 with every byte the second adds: `exact:id@ts,counts`, a bitemporal key, `bloom:id@0.01@ts`, a fixed proposal with bounds, `ColumnType` spellings and a CSV index with `source.dialect`. `format_compat.rs` replays all three fixtures (`format-v1`, `format-v0.1-pre`, `format-v0.1`) byte for byte, and `crates/bellwether-core/tests/folded_conformance.rs` pins the folded layouts' bytes against `docs/format.md` ("Folded layout conformance vectors").

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
- `docs/format.md` documents the frozen version 2 bytes: the full segment layout with a worked segment, the folded layouts, conformance vectors, the minimal manifest, the sidecar proposal vectors, and "Reading storage version 1".
- `docs/contract-v3.md` marks each entry this ADR closes.
- From M5 a change to the bytes of a segment, a manifest or a record is a new version with a new fixture directory.
- **Index kinds after the freeze are additive sidecar kinds, with no version bump.** A kind added after M5 that a frozen binary must keep working beside is declared as a sidecar (`schema.sidecars[]`, `{kind, columns, time_columns, params?}`) with its own checksummed segments listed under `segments[].kind`, never as a new field on a key spec in `schema.key_specs` and never as a change to an existing layout's bytes. The key spec it serves stays byte for byte as before, so its identity, its segments and its completeness are unchanged. A version 2 binary that does not know the kind opens the index, plans every key it knows exactly as before (it never reads the sidecar), keeps the declaration and the segments on every rewrite (unknown fields and kinds are preserved, ADR 017), and never counts them toward coverage. A build by such a binary that adds row groups leaves the sidecar incomplete; a binary that knows the kind then uses it only once complete, plans as without it meanwhile (a superset, never short, invariant 1), says so in its output, and names `bellwether index <root>`, which completes it. Tombstones (B8, `docs/format.md` "Tombstones", kind `tombstone`) are the first kind added this way, and a test runs the M5 binary (e1bd168) against a tombstone index to hold the rule (`crates/bellwether-cli/tests/tombstone_old_binary.rs`). This build holds the rule for the kind it adds, too: a `tombstone` declaration whose tombstone is unchanged is kept byte for byte (unknown fields included), and one it cannot use (params it does not read, or a key the index no longer declares on its key and time columns) is kept with its segments, listed by name in `index`, and never planned with; only `Builder::remove_spec` on a spec with a tombstone under that name removes it, and a build that declares a tombstone under its name replaces it and says so.
- **Records after the freeze: record version 4 (B8).** The storage rule above does not carry over to records. A record is read as one typed value: a version 3 reader that drops an unknown field reads the rest as if the field were absent, so a record shape whose absence means something else is not additive. Probed against the M5 binary (e1bd168) on a ledger B8 wrote under version 3, a Proposal of `temporal:id@ts,tombstone=void` read as the plain key `temporal:id@ts` and `fix --verify-only` on it passed: the tombstone was lost without a word. So B8 bumps `RECORD_VERSION` to 4; `STORAGE_VERSION` stays 2 and no storage byte changes. Version 4 adds a temporal or bitemporal key spec's `tombstone` (in a Proposal's spec and sidecars, and in a Replay's `configuration`), `Predicate::Live` (in plan Evidence residuals, Certificate row filters and a rewrite's `ConsumerPlan`), and `RecordedRun.tombstone` (workloads, which are not records and carry no record version). Every record this build writes is version 4, whatever it holds, and versions 1 to 3 are read unchanged, with their bytes and hashes (no record is rewritten). The M5 binary refuses each version 4 record before it parses it (the `record_version` peek): `fix --proposal` (with or without `--verify-only`), `fix --certificate` and `receipt show` exit 4 with "unsupported record version 4; this build supports up to 3", writing no record, manifest or segment; its ledger views list every version 4 record as one it cannot read. A live workload is refused wherever M5 loads it, because its canonical bytes no longer re-serialize ("workload … is not stored in canonical form"). `crates/bellwether-cli/tests/tombstone_old_binary.rs` runs all of these against the M5 binary. The M5 fixture (`fixtures/format-v0.1/`) is unchanged byte for byte and still replays: its records are version 3, and only the plan Evidence a lookup writes now is version 4 (`format_compat.rs` checks that this, and its hash, is all that differs). `fixtures/format-v4/` holds a live chain written at version 4, and `every_record_version_4_field_is_pinned` pins the version 4 shapes beside the unchanged version 3 goldens. A later record shape whose absence a version 4 reader would misread is a version 5 with a fixture; ADR 017's additive label fields (an older reader that drops one misreads nothing else, like `Replay.unserved`) stay additive.
