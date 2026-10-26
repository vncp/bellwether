# ADR 027: SQL tables are sources with measured costs and attest-only apply

**Status:** accepted · **Date:** 2026-10-24

## Decision

A SQL table is a `Source`. A partition or key range of the table plays the role of a file, with an identity taken from a version column, a snapshot id, or a checksum of the range's statistics; a range's statistics come from the engine where it exposes them (columnstore rowgroup metadata, range indexes, planner statistics) and otherwise from one aggregate query per range; key columns are read through ADBC where a driver exists (Postgres, SQLite, DuckDB, Flight SQL) and through ODBC otherwise (SQL Server), returning Arrow batches; the schema comes from the information schema with decimal scale and timestamp unit declared. Costs over a table are `measured`, never `exact`, and say so. Apply is attest-only: the engine verifies a rewrite and writes the receipt; a human or a CI gate deploys it, because the engine neither creates indexes inside a database nor edits jobs.

## Rejected alternatives

Treating a database as a file lake by exporting it first (hides the live system the report reads); a native driver per engine written here (ADBC and ODBC already return Arrow); pretending measured costs are exact.

## Why

Reconciliation is engine-agnostic (ADR 004) and the verb that matters to a reporting team is "the report matches the source", which needs a table on at least one side. Everything above the trait is unchanged: build, profile, replay, verify, the facade, the CLI, and the MCP server. A new source touching one trait and one crate is the test of the architecture, and this is the first one that is not a file.

## Consequences

Work package WP20. The `exact` flag on costs is insufficient for measured costs and becomes the tri-state of contract v3 (backlog entry 21); until then measured costs carry `exact: false` with the model named. `Source` gains an identity method (backlog entry 10). The evolve scorer gets a `MeasuredScorer` beside exact replay.

## Amendment, 2026-10-25 (WP20, as built)

- **No receipt without an apply.** The decision above says the engine "writes the receipt" for an attest-only apply. As built, `fix` over a table verifies, writes the Certificate, and returns the typed refusal `attest_only` naming what a human runs (`CREATE INDEX ...` per table for a key, the rewrite's text for a rewrite); it writes no Receipt. A Receipt attests that the engine applied the proposal against `manifest_before` and moved to `manifest_after` (invariant 2); nothing moved, so there is nothing for one to attest, and a Receipt for a change a human may or may not run would claim more than the engine knows.
- **Statistics are bounds from the data.** A unit's `MIN`, `MAX`, and null count come from one aggregate query per unit, in every engine. Planner statistics (`pg_stats`, `sqlite_stat4`, BRIN summaries, DuckDB zone maps, columnstore segment metadata) are samples, or describe block ranges and row groups that are not the unit, so as bounds they could exclude a row a lookup asks for (invariant 1). `bytes` are the unit's logical bytes (octet lengths and fixed widths), measured.
- **Identity.** A version column's maximum, else a checksum of the unit's statistics. No engine read in this release exposes a per-table snapshot id; the checksum's limit (an in-place update keeping every column's bounds, null count, and bytes) is stated in `format.md` and the crate README.
- **Drivers.** DuckDB, Postgres, and Flight SQL through the ADBC driver manager, which loads the engine's ADBC library at run time (for DuckDB, libduckdb itself); SQL Server through ODBC and `arrow-odbc` (feature `odbc`). SQLite through rusqlite with the bundled SQLite, opened read-only: the ADBC SQLite driver is a C library not on crates.io and types each column from its first values rather than its declared type, where this source must read the declared decimal scale and time unit.
- **Measured replays are reported, not recorded.** The ledger keeps a Replay only when its current costs are exact (ADR 005), and every cost over a table is measured, so `why` and `fix` over a table report each Replay with `exact: false` and `model: "measured: logical bytes from per-unit aggregates"` and its would-be hash, and record none. The `MeasuredScorer` (wall time, or `EXPLAIN ANALYZE` counters, with the noise as a bound) is a standalone trait in the crate until evolve (WP13) lands; nothing in the engine calls it, because it runs SQL text and a Proposal's text is an agent's.
