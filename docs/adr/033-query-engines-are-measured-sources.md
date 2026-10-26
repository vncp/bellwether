# ADR 033: Query engines are measured sources

**Status:** accepted · **Date:** 2026-10-26

## Decision

A query engine that owns its storage is a `Source` (ADR 027) through a `MeasuredEngine` adapter trait with three primitives, and only three: `submit(query, tag) -> result handle`, `stats(tag) -> QueryStats` read from the engine's own query log, and `export(handle, parquet_path)`. Identity of a table at query time is whatever the engine exposes (snapshot id, extent or partition set, table version, or a hash of its statistics). Costs are `measured`, never `exact`, with the model named (`<engine>.query_log`).

The loop for such a source:

- **Diagnose on the lake twin.** `why` runs against the Parquet copy with the agent-supplied key sample. The report gains, per candidate spec: rows per key value (fanout), files per key value, and keyset size against configurable gates (`--gate <name>:<n>`). Numbers are `exact` for the lake and the header says the diagnosis is on a copy. The planner is unchanged.
- **Record the twin's freshness.** `why --twin-of <engine uri>` records the engine's table identity beside the lake manifest sequence, so a receipt can show whether the copy was current.
- **Agent proposes a rewrite.** A *rewrite proposal* carries one or more (before, after) query pairs and the key sample, scored as a set (ADR 023: pipelines, not calls). It has no score field (ADR 009). Bellwether never emits engine SQL.
- **Engine scores.** Bellwether submits every query in the set with its own tags, reads `stats(tag)`, and records a `Replay` with `exact: false`. The agent never supplies a number. There is no `measure import` command.
- **Engine verifies.** Bellwether exports each before/after result pair to a scratch lake folder and runs `reconcile` by the proposal's key columns, with latest-by and exact decimal totals as today. No export permission means no certificate and no receipt; the refusal names the permission.
- **Record prediction error.** The receipt stores the twin's predicted rows/row groups beside the engine's realized read volume and their ratio. Over receipts this says how well the twin predicts the engine, per table, which is the number that decides whether `why` can be trusted there or the engine must be measured first (ADR 024's attribution idea applied to the twin).
- **Apply is attest-only.** The receipt binds the lake `why` evidence, the proposal (query texts hashed), the measured replay, the certificate, and the prediction error. A human or CI gate deploys. Bellwether neither edits jobs nor creates anything inside the engine.
- **Scratch governance.** Exports land in a folder the caller names, under the same storage account and ACLs as the lake; they are deleted after the certificate is written unless `--keep`; the receipt stores hashes and counts, never rows.
- **Trust boundary unchanged.** `observe.*` and `propose.*` for the agent; `score.*` and `apply.receipt` for the engine; same MCP tiers and refusal codes.

## Rejected alternatives

Importing agent-reported query statistics (a caller-supplied cost); emitting engine SQL (a language per engine, and agents already do this well); estimating engine cost from lake statistics (engine storage is not lake files; a guessed number is forbidden); exporting the whole table first (hides the live system the report reads; same rejection as ADR 027); a native driver per engine (the three primitives are all HTTP or an existing client).

## Why

The diagnosis is about data shape and transfers between engines; the measurement does not. Keeping both on the engine side of the boundary means an engine receipt reads like a lake receipt. This is also the test ADR 027 asked for: a source that is neither a file nor a SQL table, touching one trait and one crate.

## Consequences

Work package WP20b; crate `bellwether-source-engine` (adapters ClickHouse, the reference, tested end to end against a server in CI; and Kusto, whose tests skip without a cluster). BigQuery (`INFORMATION_SCHEMA.JOBS`, `EXPORT DATA`), Snowflake (`QUERY_HISTORY`, `COPY INTO`), and Postgres (`EXPLAIN (ANALYZE, BUFFERS)`, `COPY`) are each a one-file adapter, documented in the crate README and not written. Contract changes, all additive and absent from records that do not use them (their hashes are unchanged): `RunCost.measured` (`QueryStats`), `Replay.prediction` and `Receipt.prediction` (`Prediction`), `Receipt.attest_only`, `RewriteShape.pairs`, `.key`, `.keys`, `EvidenceBody::Profile.twin` (`TwinOf`, with `Gate`), and `Error::ExportDenied`. Ledger rules: a Replay may carry measured current costs only for a rewrite and only when it names its model; a cost carrying engine statistics is never exact; a prediction belongs to a measured rewrite; a receipt over a measured replay must be attest-only, an attest-only receipt is a rewrite's and leaves the manifest unchanged, and a receipt's prediction is its replay's. Kill criterion for the four twin lints: rule catalog, "Twin lints".

## Amendment, 2026-10-26 (WP20b, as built)

- **Attest-only receipts differ from ADR 027's table runs.** ADR 027's amendment ends a `fix` over a SQL table with the typed refusal `attest_only` and no Receipt, because nothing moved. An engine rewrite ends in a Receipt marked `attest_only: true` with `manifest_before == manifest_after`, because here the Receipt is the record that binds the measured Replay, the Certificate, and the prediction error, which a reviewer compares across receipts; the marker says, in the record itself, that the engine applied nothing. The SQL path is to be aligned to the same attest-only Receipt in a follow-up once both have merged, so reviewers compare one receipt shape.
- **The lake is the root; the engine is named, never given by an agent.** Every command runs on the lake folder; `--twin-of <uri>` (an engine option, the same flag for the CLI and the MCP server) names the engine, and `--scratch <folder>` and `--keep` are the operator's. An MCP tool argument never names an engine, a connection, or a scratch folder. The only query text bellwether submits is a rewrite proposal's pairs and `workload record`'s keys query, to that engine only, under the engine's own read-only setting (ClickHouse `readonly=2`, Kusto `request_readonly`), each under a tag bellwether chose.
- **Identity.** ClickHouse: one unit per partition of each MergeTree table (rows, the lowest and highest block numbers, the highest mutation version), so inserts, deletes, and mutations change it and a background merge does not. Kusto: one unit per extent (`.show table extents`), so a merge changes it too and a run across a merge refuses (`source_changed`) rather than certifies. The identity is read before scoring and after the exports; a change between them is refused.
- **Twin pass cost.** The fanout, files-per-key, and prediction numbers come from one pass that reads the key sample's columns from every live row group of the lake, bounded per row group (ADR 018); the report states the rows and row groups it read.
