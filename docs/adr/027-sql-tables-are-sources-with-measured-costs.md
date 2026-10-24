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
