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
| WP12 mcp | merged: observe, score, propose, apply, and jobs tools; resources; the prompt generated from `AGENTS.md`; typed refusals, dry runs, per-session budgets; ADR 010 against the CLI |
| WP20 sql | in review: `bellwether-source-sql`, `sql://` roots through the facade, CLI, and MCP; reconcile a table against files; measured `why`; attest-only `fix` |
| WP20b engines | in review: `bellwether-source-engine` (ADR 033), the `MeasuredEngine` trait with ClickHouse (reference, CI container) and Kusto adapters; `workload record --from-engine`; `why --twin-of --gate` with the twin lints; `propose --rewrite`; `fix` scoring, exporting, reconciling, and attesting a rewrite of engine queries |
| WP13a advise | after M3; no language model (ADR 032) |
| WP13b evolve, WP13c rulebook v2 | each opens only when its measured trigger in `docs/evolve.md` §3 is met |
| WP14 storage version 2 and record version 3 (ADR 028) | both pull requests done, **M5 declared**: the first brought per-key counts (`exact:col,counts`, `count`), checksummed segments, manifest row-group and column bytes (plans open no footer), identity keys and every record version 3 shape; the second filled the remaining record fields, folded the sidecars (`exact:col@ts` bounds in the value record, a `bitemporal` layout, both proposable and fixable), spelled column types and the CSV dialect in the manifest, and froze the bytes. Fixtures: `fixtures/format-v1/`, `fixtures/format-v0.1-pre/` (pre-M5), `fixtures/format-v0.1/` (M5) |
| WP15 relational replay, WP17 CI receipts, WP18 transfer evaluation | planned 2026-10-24 (ADR 026) |
| WP16 engine adapters | merged: `bellwether-adapters`, plans executed inside engines through `locate`, `latest`, `asof --emit`: DataFusion 55 with row-group access plans (data bytes equal the plan's at the storage boundary), one DuckDB statement over the planned files (row groups not pinnable; bytes measured), a Spark manifest and shim (planned files only); the residual decoded at the dataset types and run in the plan's order, or refused by name |
| WP19 signed receipts | merged in wave A: detached ed25519 signatures beside receipts and certificates (ADR 029), no record or format version change; M8's signing half done |
| WP21 time-series keys | merged: partition values from Hive-style paths, ordered plans, `--at-manifest` pinned lookups over a manifest history, per-key time bounds (`bounds` sidecar), bitemporal as-of (`temporal2` sidecar), the `market` bench corpus with measured funnels |
| WP22 performance | next after WP21; measured in release builds |
| B2 sketch | phase 5: per item, per file HyperLogLog registers and optional MinHash signatures as a `sketch` sidecar in `schema.sidecars[]`, approximate and never read by a planner; `cardinality`, `overlap`, the `why` `sketch` column and `--sketches-only`, the low-cardinality-key detector `R-sketch-1`; benched on the ledger and `market` shapes over 10⁴ files |

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
| WP12 | `bellwether-mcp` | after WP9; tools in tiers (`observe.why/profile/locate/asof`; `score.why/verify`, engine-scored; `propose.key/layout/rewrite`; `apply.receipt`; `jobs.start/status/result`); resources (findings, receipts, scorecard); a "diagnose a slow query" prompt generated from `AGENTS.md`; typed schemas generated from `bellwether-types` through an off-by-default `schema` feature (non-semantic); refusal codes; dry runs; per-session budgets; single static binary; observe-tool latency budget enforced | ADR 009, 010; threat model |

## Row 4 · after M3

WP13 is three packages in `bellwether-evolve`, owning ADR 021–025, 031, 032 and the threat model extension in `docs/evolve.md` §9. Each has a done-when the ledger can prove and a trigger that opens the next (ADR 032; the triggers, the kill criterion, and the stage-2 budget are in `docs/evolve.md` §3).

| WP | Scope | Done when | Opens the next WP when |
| --- | --- | --- | --- |
| 13a · `advise` | `Layout` and `Cadence` artifacts in the contract; rule-driven enumeration; exact knob descent; materialization-set search; Pareto over runs × objectives with anchors and δ; lineage-edge attribution; bisect-on-failed-certificate; `bellwether advise` CLI and MCP tier `score.advise`; receipts for every admitted change | On the synthetic ledger, every planted anti-pattern reachable by enumeration or descent is recovered blind; on one real workload, `advise` produces a frontier with receipts and the realized-vs-predicted figure lies on the diagonal | Planted or real anti-patterns remain that need a structural rewrite (operator reorder, cross-step push, fusion) |
| 13b · `evolve` | ADR 031 proposers behind `--llm`; generalization lint; deterministic composer from the interaction matrix; cheap-proposer default with strong composer on non-additive bundles; `Experience` records and reflection; proposer prompts, completions and model ids in the ledger; replay-from-ledger byte-for-byte | Stage 2 recovers the anti-patterns 13a could not, and cost per accepted mutation (replays + model calls) is under the budget written in §3 | A winning mutation recurs across two or more templates |
| 13c · rulebook v2 | Templates with predicates and re-fit parameters; temporal holdout; `helpful`/`harmful` counters; bucket caps with merge; staleness re-earn; `bellwether rules {ls, audit, retire}` | One rule learned on the search templates reproduces a gain on a temporally held-out template under replay, and the transfer figure is rendered from ledger records | — |

**Order inside 13a.** Contract additions first (`Layout`, `Cadence`, `Attribution` over edges, `Anchor`), then enumeration and descent, then Pareto with anchors, then receipts and the CLI. Attribution over edges comes before any proposer so the proposers in 13b start with the right evidence.

**Carried into 13a from the single WP13**, because every stage stands on them: the `Scorer` and `Verifier` traits (replay and reconcile first; `docs/evolve.md` §15); the plan DAG model with named rewrite identities; the pipeline DAG with lineage and output-level certificates (ADR 023); counterfactual attribution and the interaction matrix over plan nodes (ADR 024), which edge attribution extends; set-valued physical design with subsumption and maintenance cost; the crowding-distance tie-break; budgets and seed pinning so a search replays from its ledger; the SLOs from `docs/evolve.md` §12 asserted in bench. `bellwether report` renders each figure in `docs/evolve.md` §11 (HTML charts, and the `--paper` CSV export matching `paper/data/README.md`) when its records exist; no figure is a delivery gate.

## Row 5 · after M4, before the format freeze

| WP | Crate | Deliverables | Owns |
|---|---|---|---|
| WP14 | `bellwether-types`, `-core`, `-store`, `-build`, `-ledger`, `-replay`, `-profile`, `-verify`, the facade, `-cli` | storage version 2 and record version 3 in two pull requests. First: optional per-key, per-row-group counts in exact and temporal segments with `count`; checksums; per-row-group rows and bytes and per-column bytes in manifests, so plans open no footer; `ScanEntry.columns` and `projected_bytes`; full-spec identity keys; every record version 3 shape; conformance vectors; the version 1 and version 2 fixtures; readers of version 1 unaffected, upgraded on the first build with work. Second: verify, replay and profile fill the record fields; sidecars fold into the segment format; typed CSV dialect and column types in manifests | ADR 026, 028; format.md |
| WP15 | `bellwether-replay` | the operator table in `docs/evolve.md` §14 priced exactly: projection, range, `latest`/`asof`, semi-join, two-sided join with cardinality, window partition prefilter, aggregation input; tri-state exactness on every cost; layout proposals exact under the stated model | ADR 005, 026 |
| WP16 | `bellwether-adapters` (merged) | plans executed inside engines: DataFusion first (row-group access plans; realized data bytes equal the plan's, measured at the storage boundary), DuckDB second (file lists plus pushed filters; row groups cannot be pinned, so its bytes are measured as an upper bound of the plan's), a Spark shim third (file level: only the planned files are opened); rows and the exact decimal total equal the bench oracle's in every engine | ADR 010 |
| WP17 | `bellwether-ci` | a GitHub Action that runs `why` on a pull request's pipeline changes and posts the receipt or the refusal as a comment; the gate is the receipt hash | threat model |
| WP18 | evaluation | transfer: the rulebook learned on one dataset applied to another and validated by replay there; the exact-versus-estimated-loss ablation at equal budget; public datasets (ClickBench slices, GH Archive, NYC taxi) beside the synthetic ledger | ADR 019, 025 |
| WP19 | `bellwether-ledger` (done) | signed receipts: detached ed25519 signature objects beside receipts and certificates (`ledger/<hash>.sig.<key id>.json`), `fix` signs what it wrote, a reader's trust file with validity windows, verdicts in `receipt show`, `explain`, `ledger receipts`, `report`, and the MCP envelope, and `receipt show --require-signature`; keyless (OIDC) signing deferred because it is a network call inside `fix` | ADR 006, 029; threat model |
| WP21 | time-series keys (merged) | built before storage version 2 as additive, v1-compatible changes: partition values from Hive-style paths as statistics; plans ordered by a timestamp range key; `--at-manifest` pinned lookups; per-key time bounds as a sidecar segment kind (`exact:symbol@ts`) so symbol × window prunes exactly; bitemporal `temporal2:key@valid,known` with `asof --valid --known`; a `market` bench corpus and workload; folded into storage version 2 when it lands | ADR 014, 020; format.md additive sidecar kinds |
| WP22 | performance | release-build measurements on the `market` corpus and the public datasets: lookup p50 and p95 for symbol-set and window queries against a full scan and against DuckDB on the same files; build throughput in row groups per second with its scaling curve from one core to all; reconcile rows per second per side and the budget that avoids re-reading; hot paths profiled and the measured wins taken (bitmap operations, bloom probing, footer caching, parallel reconcile passes); then the multi-machine build: N workers writing content-addressed segments, one committer assigning id ranges and committing once | ADR 018, 019 |
| B2 | `bellwether-core`, `bellwether-build`, `bellwether-replay`, `bellwether-profile`, facade, CLI, MCP | `sketch:col[,col][,a+b][,p=14][,minhash=K]`: the operator is `count(distinct col)` ("is this column a key"); per item, per file HyperLogLog registers (default `p = 14`) and an optional MinHash signature of `k` slots, in a sidecar segment (`BWSKETCH`) declared in `schema.sidecars[]`; labelled approximate in every type, output, and doc, and never read by any planner path (tested); `cardinality(item)` with a 99.7 % interval (exact where a complete `exact` key spec holds the count), `overlap(a, b)` as an approximate Jaccard; a transient sketch for unindexed folders; `why` gains the `sketch` column (rows per value per candidate), `reads`, and `--sketches-only` (no data file read); detector `R-sketch-1`, the low-cardinality key; bench: `scripts/sketch-funnels.py` on the ledger (one day, 10⁴ files) and `market` (10 days × 1,000 files) shapes, data files read by `why` with and without sketches | invariant 1 (no approximate number in a plan); ADR 014, 016, 017, 018; format.md additive sidecar kind |
| WP20 | `bellwether-source-sql` | runs right after WP12: a `Source` over SQL tables per ADR 027 (ADBC for Postgres, SQLite, DuckDB, Flight SQL; ODBC for SQL Server); partitions or key ranges as the unit, identity from a version column or snapshot, statistics from the engine or one aggregate query per range, projected reads as Arrow; `reconcile` between a table and a folder of files as the headline, with the certificate naming both identities; `why` over a table with every cost labeled measured; a `MeasuredScorer` for evolve; attest-only apply; tests on SQLite and DuckDB in-process, Postgres when a server is available, SQL Server when an ODBC driver is present, each skipping with a clear message otherwise | ADR 027; contract v3 entries 10 and 21 |
| WP20b | `bellwether-source-engine` | right after WP20, reusing `MeasuredScorer` and the `Source` identity method: query engines that own their storage as measured sources (ADR 033) through a `MeasuredEngine` adapter of three primitives (`submit(query, tag)`, `stats(tag)` from the engine's own query log, `export(handle, dir)`); ClickHouse the reference adapter, tested end to end in a CI container; Kusto the second, skipping without a cluster; BigQuery, Snowflake, and Postgres documented as one-file adapters; `workload record --from-engine --keys-query`; `why --twin-of --gate` with fanout, gate, and freshness columns and four twin lints; rewrite proposals of (before, after) pairs scored as a set; `fix` scores, exports to a caller-named scratch folder, reconciles, and writes an attest-only receipt with the prediction error, kept even when the certificate fails | ADR 027, 033; format.md "Query engine sources" |

## Milestones

1. **M1** `why` works with no index: WP0, WP2, WP3, WP5, WP10, minimal WP9.
2. **M2** the index exists: WP1, WP4. `locate`, `latest`, `asof` on the synthetic ledger.
3. **M3** receipts exist: WP6, WP7, WP8. `fix` produces a Receipt.
4. **M4** agent-first: WP12. The loop runs end to end from a coding agent.
5. **M5** (done) format frozen after WP14, so the frozen version carries counts and per-column bytes; fixtures committed. The first WP14 pull request made the record version 3 shapes final and committed the pre-M5 fixture (now `fixtures/format-v0.1-pre/`); the second froze the bytes of storage version 2 with `fixtures/format-v0.1/` (ADR 028).
6. **M6** staged (ADR 032): `advise` (WP13a) produces a frontier with receipts on the synthetic ledger and one real workload, with no language model; `evolve` (WP13b) ships only if `advise` leaves anti-patterns that need a structural rewrite, and recovers them under the budget in `docs/evolve.md` §3; rulebook v2 (WP13c) ships once a rule reproduces its gain on a temporally held-out template. Each stage is a replayable run in the ledger; WP15 makes the loss cover the operator table.
7. **M7** a SQL table reconciles against a folder of files with a certificate (WP20); plans execute inside an engine (WP16); receipts gate pull requests (WP17).
8. **M8** rules transfer across datasets and the exact-versus-estimated ablation is measured (WP18); receipts are signed (WP19, done: `fix --signing-key`, `receipt show --require-signature`).

## Dependency upgrade (start of Phase 3)

Before WP8, WP9, and WP12 begin: upgrade `object_store` from 0.11 to the current release and `arrow`/`parquet` from 53 to the current release in one PR, re-run every crate's tests, sweeps, and the conformance test, and remove every advisory ignore in `deny.toml` (quick-xml, rustls-pemfile, paste, and instant from the optional DataFusion baseline). Deferred from Phase 2 so that build, profile, and replay are written against one pinned API.

Done 2026-10-24. Arrow and parquet went to 59, not 60: 60.0.0 was the newest release, but DataFusion 55.2, the newest DataFusion, is built on arrow 59, and the workspace keeps one arrow. object_store went to 0.14.2. The type-spelling, catalog-digest, and statistics amendments are in `format.md`, and the re-pinned Parquet-byte goldens are in `benchmark.md`.

## Contract v3 backlog

Gaps the Phase 2 crates found in the contract and the docs, with interim behaviour and proposed resolutions, are in [`contract-v3.md`](contract-v3.md).

## Deferred, with reasons

- Further engine adapters (Polars): DataFusion, DuckDB, and Spark shipped in WP16 (`crates/bellwether-adapters`); Polars until a user asks.
- CI integration (PR lint action): the lint exists in WP5; wiring it to a forge is separate.
- SQL sources: scheduled as WP20 (ADR 027) now that the `Source` trait has survived two real sources.
- Delta and Iceberg manifest-aware discovery: for now, folders of Parquet.
- Learned components (churn-predicted layout, learned bloom oracle): research track; needs M3 data.
- Learned or adaptive index structures: an approximate number must never reach a planner path (invariant 1); sketches (B2) are the only approximate structure and they never prune.
- Distributed build: segments are already independent; a coordinator is a later convenience.
- Further query engine adapters (BigQuery, Snowflake, Postgres, Redshift) beyond ClickHouse and Kusto: until a user asks. The `MeasuredEngine` trait documentation and the one-file sketches in `crates/bellwether-source-engine/README.md` are the deliverable.
- Geo kinds: no workload asks for one; an `expr` key over a cell id (B6) plus `exact` covers the point case when one appears.
