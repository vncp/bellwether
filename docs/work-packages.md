# Work packages

One crate, one owner, one interface contract each. WP0 ships first and alone. After it, packages in the same row have no dependency on each other and are built concurrently, each on its own branch, each landing by PR reviewed against the invariants and ADRs it owns. Done means: public API matches the contract in `bellwether-types`, assigned invariants have tests, a docs page exists, and a benchmark is registered if the crate has a latency budget.

## Status (2026-10-24)

| Work | State |
|---|---|
| WP0 types | merged; contract v2 |
| WP1 core, WP2 source, WP3 store, WP7 ledger | merged, each with its reviewer's probe tests as permanent regression tests |
| WP10 bench | merged |
| WP11 docs | ongoing |
| WP4 build | merged after a four-item review round; its probes are regression tests |
| WP6 replay | merged after two review rounds; PlanSource implemented for the build planner |
| WP5 profile | merged after four review rounds; one shared decision module with the bench |
| WP8 verify | merged after four review rounds; no false pass over 12,800 chunkings |
| WP9 facade and cli | merged: why, index, locate, latest, asof, propose, reconcile, fix, receipt, report; profile wired into `why` |
| Dependency upgrade | merged: arrow/parquet 59, object_store 0.14, DataFusion 55 (bench feature); `deny.toml` has no advisory ignores |
| WP12 mcp | in progress |
| WP20 sql | next |
| WP13 evolve | after M3 |
| WP14 counted segments and column bytes, WP15 relational replay, WP16 engine adapters, WP17 CI receipts, WP18 transfer evaluation, WP19 signed receipts | planned 2026-10-24 (ADR 026) |
| WP20 SQL sources | planned 2026-10-24 (ADR 027); runs right after WP12 |
| WP21 time-series keys | in progress, concurrently with WP12 |
| WP22 performance | next after WP21; measured in release builds |

## WP0 · first, alone

`bellwether-types`. Done.

## Row 1 · fully parallel

| WP | Crate | Deliverables | Owns |
|---|---|---|---|
| WP1 | `bellwether-core` | memcomparable encoding for all Arrow scalar types (property test: encoded order equals typed order); segment writer/reader with sorted key blocks, SBBF bloom per block (xxhash64), key → roaring bitmap; temporal chains with `latest(keys)` and `asof(keys, ts)`; multi-segment merge | invariants 1, 5; ADR 001, 002, 008, 014, 016 |
| WP2 | `bellwether-source` | `ParquetSource` (footer parse, row-group stats, projected column reads by byte range, reuse embedded blooms); `ChunkedTextSource` for CSV and JSON lines; fingerprinting; a fixture directory of small Parquet files with known stats used by every other crate | ADR 007, 013 (fingerprints) |
| WP3 | `bellwether-store` | local, S3, ADLS, GCS via `object_store`; conditional PUT per backend; manifest commit protocol (read version, write segments, CAS manifest, rebase on conflict); content-addressed cache with age | invariant 4; ADR 003, 015, 017 |
| WP7 | `bellwether-ledger` | append-only records named by blake3 of canonical JSON; write-time rules (Receipt needs a resolving, passing Certificate for its proposal computed against `manifest_before`, and `manifest_before < manifest_after <= current` by manifest sequence; Evidence and Certificates need provenance); `explain(receipt)` walks the chain | invariants 2, 6; ADR 006 |
| WP10 | `bellwether-bench` | synthetic versioned-ledger generator (entities with version churn, a batch-stamp column, a true id, partition-by-day layout; parameters for files, row groups, cardinality, churn); recorded workload generator at p50/p95/max sizes; harness over raw scan and the index emitting JSON for funnel and latency charts; SLO assertions; planted anti-patterns as ground truth | ADR 018 (budgets), 019 |
| WP11 | docs | README three-screen structure; rule catalog skeleton (detector, estimator, guidance template, verification method, confidence rubric per lint); keep `architecture.md` current | — |

## Row 2 · after WP1, WP2, WP3

| WP | Crate | Deliverables | Owns |
|---|---|---|---|
| WP4 | `bellwether-build` | discover → fingerprint → assign ids → per-worker segment → one manifest commit; incremental by fingerprint; tombstones; compaction; key-spec completeness recorded in manifest | ADR 012, 013 |
| WP5 | `bellwether-profile` | fanout per column (HLL distinct, files and row groups per value at p50/p95/max, sampled by distinct value when sampled); sortedness and range-gap per column; lints (batch-stamp key, low-selectivity single column, declared-but-unused key, scattered time column); candidate composite enumeration | invariant 6 (emits provenance) |
| WP6 | `bellwether-replay` | workload recorder (key sets per run into cache with hash); exact replay of current and hypothetical `KeySpec` via a transient in-memory segment over key reads; `Replay` records | invariant 3; ADR 005 |

## Row 3 · after row 2

| WP | Crate | Deliverables | Owns |
|---|---|---|---|
| WP8 | `bellwether-verify` | reconcile two sources by key with latest-by, row hash per ADR 016, exact decimal totals by group; scoped mode over a recorded workload with coverage statement; `Certificate` records | ADR 004, 016 |
| WP9 | `bellwether` (facade) and `bellwether-cli` | the facade: `Engine::open`, every operation as a typed serde result, `fix` as score → verify (certificate against `manifest_before`, a Key proposal verified by reconciling the full scan against a `Source` over the hypothetical plan) → apply → attest; the CLI: `why`, `index`, `locate`, `latest`, `asof`, `reconcile`, `propose`, `fix`, `receipt show`, `report`, `bench`; human and `--json` output everywhere; terminal funnel and fanout table; HTML report with inline SVG rendered from ledger records | ADR 010, 017 |
| WP12 | `bellwether-mcp` | after WP9; tools in three tiers (`observe.why/profile/locate/asof`, `propose.key/layout/rewrite`, `apply.receipt`); resources (findings, receipts, scorecard); a "diagnose a slow query" prompt; typed schemas generated from `bellwether-types` through an off-by-default `schema` feature (non-semantic); single static binary; observe-tool latency budget enforced | ADR 009, 010; threat model |

## Row 4 · after M3

| WP | Crate | Deliverables | Owns |
|---|---|---|---|
| WP13 | `bellwether-evolve` | Per `docs/evolve.md`: `Scorer` and `Verifier` traits (replay and reconcile first); plan DAG model with named rewrite identities; counterfactual attribution and interaction matrix (ADR 024); three proposers: LLM textual-gradient behind a trait with every prompt and completion as ledger records, exact finite-difference descent for knobs, rule-driven enumeration (ADR 022); set-valued physical design with subsumption and maintenance cost; pipeline DAG with lineage and output-level certificates (ADR 023); Pareto selection over runs × objectives with crowding distance; budgets and seed pinning so a search replays from its ledger; rulebook distillation validated on held-out runs (ADR 025); `bellwether evolve` and `bellwether report` (HTML charts plus `--paper` CSV export matching `paper/data/README.md`) with funnel, latency, realized-vs-predicted, convergence, frontier evolution, loss-landscape surface and contour with descent path, attribution and interaction heatmaps, and transfer charts; SLOs from `docs/evolve.md` §12 asserted in bench | ADR 021–025; threat model |

## Row 5 · after M4, before the format freeze

| WP | Crate | Deliverables | Owns |
|---|---|---|---|
| WP14 | `bellwether-core`, `bellwether-build` | storage version 2: optional per-key, per-row-group counts in exact segments; per-column bytes in manifests and scan entries; conformance vectors for both; readers of version 1 unaffected | ADR 026; format.md |
| WP15 | `bellwether-replay` | the operator table in `docs/evolve.md` §14 priced exactly: projection, range, `latest`/`asof`, semi-join, two-sided join with cardinality, window partition prefilter, aggregation input; tri-state exactness on every cost; layout proposals exact under the stated model | ADR 005, 026 |
| WP16 | `bellwether-adapters` | plans executed inside engines: DataFusion first (row-group access plans), DuckDB second (file lists plus pushed filters), a Spark shim third; each adapter's realized bytes must equal the plan's | ADR 010 |
| WP17 | `bellwether-ci` | a GitHub Action that runs `why` on a pull request's pipeline changes and posts the receipt or the refusal as a comment; the gate is the receipt hash | threat model |
| WP18 | evaluation | transfer: the rulebook learned on one dataset applied to another and validated by replay there; the exact-versus-estimated-loss ablation at equal budget; public datasets (ClickBench slices, GH Archive, NYC taxi) beside the synthetic ledger | ADR 019, 025 |
| WP19 | `bellwether-ledger` | signed receipts (ed25519 keys or keyless signing), so a receipt is tamper-proof rather than tamper-evident | ADR 006; threat model |
| WP21 | time-series keys | built before storage version 2 as additive, v1-compatible changes: partition values from Hive-style paths as statistics; plans ordered by a timestamp range key; `--at-manifest` pinned lookups; per-key time bounds as a sidecar segment kind (`exact:symbol@ts`) so symbol × window prunes exactly; bitemporal `temporal2:key@valid,known` with `asof --valid --known`; a `market` bench corpus and workload; folded into storage version 2 when it lands | ADR 014, 020; format.md additive sidecar kinds |
| WP22 | performance | release-build measurements on the `market` corpus and the public datasets: lookup p50 and p95 for symbol-set and window queries against a full scan and against DuckDB on the same files; build throughput in row groups per second with its scaling curve from one core to all; reconcile rows per second per side and the budget that avoids re-reading; hot paths profiled and the measured wins taken (bitmap operations, bloom probing, footer caching, parallel reconcile passes); then the multi-machine build: N workers writing content-addressed segments, one committer assigning id ranges and committing once | ADR 018, 019 |
| WP20 | `bellwether-source-sql` | runs right after WP12: a `Source` over SQL tables per ADR 027 (ADBC for Postgres, SQLite, DuckDB, Flight SQL; ODBC for SQL Server); partitions or key ranges as the unit, identity from a version column or snapshot, statistics from the engine or one aggregate query per range, projected reads as Arrow; `reconcile` between a table and a folder of files as the headline, with the certificate naming both identities; `why` over a table with every cost labeled measured; a `MeasuredScorer` for evolve; attest-only apply; tests on SQLite and DuckDB in-process, Postgres when a server is available, SQL Server when an ODBC driver is present, each skipping with a clear message otherwise | ADR 027; contract v3 entries 10 and 21 |

## Milestones

1. **M1** `why` works with no index: WP0, WP2, WP3, WP5, WP10, minimal WP9.
2. **M2** the index exists: WP1, WP4. `locate`, `latest`, `asof` on the synthetic ledger.
3. **M3** receipts exist: WP6, WP7, WP8. `fix` produces a Receipt.
4. **M4** agent-first: WP12. The loop runs end to end from a coding agent.
5. **M5** format frozen after WP14, so the frozen version carries counts and per-column bytes; fixture committed.
6. **M6** `evolve`: reflective workload optimization over the synthetic ledger, with the rulebook as output and a replayable run in the ledger; WP15 makes the loss cover the operator table.
7. **M7** a SQL table reconciles against a folder of files with a certificate (WP20); plans execute inside an engine (WP16); receipts gate pull requests (WP17).
8. **M8** rules transfer across datasets and the exact-versus-estimated ablation is measured (WP18); receipts are signed (WP19).

## Dependency upgrade (start of Phase 3)

Before WP8, WP9, and WP12 begin: upgrade `object_store` from 0.11 to the current release and `arrow`/`parquet` from 53 to the current release in one PR, re-run every crate's tests, sweeps, and the conformance test, and remove every advisory ignore in `deny.toml` (quick-xml, rustls-pemfile, paste, and instant from the optional DataFusion baseline). Deferred from Phase 2 so that build, profile, and replay are written against one pinned API.

Done 2026-10-24. Arrow and parquet went to 59, not 60: 60.0.0 was the newest release, but DataFusion 55.2, the newest DataFusion, is built on arrow 59, and the workspace keeps one arrow. object_store went to 0.14.2. The type-spelling, catalog-digest, and statistics amendments are in `format.md`, and the re-pinned Parquet-byte goldens are in `benchmark.md`.

## Contract v3 backlog

Gaps the Phase 2 crates found in the contract and the docs, with interim behaviour and proposed resolutions, are in [`contract-v3.md`](contract-v3.md).

## Deferred, with reasons

- Engine adapters (DataFusion `ParquetAccessPlan`, DuckDB, Polars, Spark shim): `ScanPlan` is designed for them; they wait until the plan type is stable.
- CI integration (PR lint action): the lint exists in WP5; wiring it to a forge is separate.
- SQL sources: scheduled as WP20 (ADR 027) now that the `Source` trait has survived two real sources.
- Delta and Iceberg manifest-aware discovery: for now, folders of Parquet.
- Learned components (churn-predicted layout, learned bloom oracle): research track; needs M3 data.
- Distributed build: segments are already independent; a coordinator is a later convenience.
