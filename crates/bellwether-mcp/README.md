# bellwether-mcp

Work package WP12. The engine's agent surface: an MCP server over stdio, bound to one data folder, built on the official Rust MCP SDK (`rmcp`, pinned). It owns ADR 009 (the agent proposes; only the engine scores, verifies, and applies) and the [threat model](../../docs/threat-model.md). The binary is `bellwether-mcp`; the same server is `bellwether mcp`.

Dependencies, as `dev/check-layering.py` permits: `bellwether` (the facade; every tool is one call into it) and `bellwether-types` (the contract, with its `schema` feature: every tool's input schema is generated from the contract's types).

## For a coding agent

Add the server to the agent's MCP configuration (the `mcpServers` block most clients read), naming the folder it is bound to:

```json
"bellwether": {
  "command": "bellwether", "args": ["mcp", "/path/to/folder"]
}
```

Then ask the agent to run the prompt `diagnose_slow_query`. The prompt is [`AGENTS.md`](../../AGENTS.md) at the repository root, embedded at build time: the loop in order, every refusal code, and what the agent may never do.

Flags: `--index <PATH|URL>` (default `<ROOT>/.bellwether`), `--format parquet|csv|jsonl`, `--chunk-size`, `--schema <file>`, `--fingerprint-policy`, `--partitioning none|hive` or `--path-template <PATTERN>`, and the session budget `--budget-replays N` (default 256), `--budget-rows N` (default 2,000,000,000), `--budget-seconds N` (default 3,600). `bellwether mcp` takes the same flags from the same definitions (`bellwether_mcp::args`), so the two open a folder the same way (an unindexed CSV or JSON lines folder falls back to `--schema`, `--chunk-size`, and `--fingerprint-policy`, and an unindexed partitioned folder to `--partitioning` or `--path-template`). Exit codes: 0 when the client disconnects; 2 usage (including a `--schema` file that cannot be read or a malformed `--path-template`); 4 the engine could not open the folder (before the handshake, nothing on stdout) or the transport failed.

A folder that is the lake twin of a query engine (ADR 033) is served with `--twin-of <clickhouse://...|kusto://...>` (a secret only as `password=env:NAME`), `--scratch <DIR>` (where the engine's exports land), and `--keep` (keep them after the certificate). These are the operator's: no tool argument names an engine, a connection, or a folder to write, so an agent can reach only the engine the human named.

## Tools

|Tier|Tool|Facade call|Writes|
|---|---|---|---|
|observe|`observe.why` (no keys)|`why` without a workload|the profile Evidence, cache entries|
|observe|`observe.profile`|`findings`|nothing|
|observe|`observe.cardinality` (`items`, `transient`), `observe.overlap` (`left`, `right`)|`cardinality`, `overlap`: approximate distinct counts and Jaccard similarity from `sketch` sidecars (never used to plan); `transient` reads the files for items no sketch holds|nothing|
|observe|`observe.text` (`column`, `op`: `has_token`, `contains`, `has_prefix`, `value`; optional `at_manifest`)|`locate_text`: the plan through the column's text keys, saying when it is a candidate set and the residual is mandatory|the `plan` Evidence, cache entries|
|observe|`observe.locate`, `observe.asof` (`at`, or the bitemporal `valid` and/or `known`; exactly one form; `valid_by` with `known_by` pick the bitemporal key by its clock columns when several serve the key columns)|`locate`, `asof`, `asof_bitemporal`; with `at_manifest`, on an engine opened at that committed manifest (`EngineOptions::at_manifest`, the CLI's `--at-manifest`)|the `plan` Evidence, cache entries|
|score|`score.why` (keys, a workload hash, or `text` lookups, each recorded as one text run; `gates` with a twin; `valid_by` and `known_by`, with optional `valid` and `known`, record a bitemporal run, as `why --valid-by --known-by`)|`why` with a workload|engine Proposals and Replays; with a twin, the profile Evidence records it|
|score|`score.verify` (`engine` sets latest-by, ignored columns, and totals for a rewrite of engine queries)|`fix` with `verify_only`|Replay, plan Evidence, Certificate; for an engine rewrite the measured Replay, then the Certificate|
|score|`score.record` (`keys_query`, `key_columns`; needs `--twin-of`)|`record_from_engine`|a workload in the cache|
|propose|`propose.key` (`spec`: a key spec as JSON, or its short form as `index --key` reads it, such as `exact:a@ts,counts` or `temporal2:id@valid,known`; `sidecars`: specs applied beside it), `propose.layout`, `propose.rewrite`|`propose_key`, `propose`, or their `_dry_run` with `dry_run`; a sketch declaration is refused with `sketch_not_proposable`|one Proposal, or nothing|
|apply|`apply.receipt` (a certificate hash only)|`fix` with the certificate, or `apply_dry_run` with `dry_run`|the index commit, the realized Replay, the Receipt; nothing on a dry run or a refusal, unless another commit lands during the apply (`concurrent_commit`)|
|jobs|`jobs.start`, `jobs.status`, `jobs.result`|the started tool's|the started tool's|

Every input schema is closed (`additionalProperties: false` at every depth). A score, cost, measurement, replay, prediction, verdict, or certificate field anywhere in any tool's arguments (`refusal::COST_FIELDS`), called directly or started with `jobs.start`, is refused by name with `caller_supplied_cost`; any other unknown field is `invalid_input`. No tool accepts a path to write or a shell command; the paths a tool accepts (a key file, a rewrite's output folders) are only read. The one query text a tool takes is `score.record`'s `keys_query` and a rewrite's `shape.pairs`: the engine submits it read-only, under its own tags, to the engine `--twin-of` names, never to the lake or a shell.

Each reply's text is the facade's typed result serialized exactly as the CLI's `--json` line prints it (ADR 010). The structured content is an envelope: the tool and tier, `status` (`ok`, `refused`, `running`), `cites` (every record hash in the result, with its kind), `chains` (the explain chain of every receipt the result mentions, and whether it is intact), `refusal` (a stable `code`, the facts to act on, one `next` line naming MCP tools and fields, never a CLI flag), `job`, and the session `budget`.

**Refusals.** Every facade error and typed refusal maps to its code by an exhaustive match, so a new variant fails to compile until it is mapped. The errors an agent meets in the loop are typed in the facade and carry the argument to fix: `not_found` and `wrong_kind` name the hash's kind and field, `no_workload` the missing `workload` or `rewrite`, `not_indexed` the key spec a lookup needs, `not_retained` the oldest and current manifest sequences an `at_manifest` pin may name, `invalid_argument` the field (`by`, `evidence`, `at`, `keys`, `key_columns`, `cluster_by`, `shape`, `workload`, `rewrite`), and `invalid_spec` for an unknown column carries the column and the dataset's columns. `engine_error` is left for the rest, whose message says what happened; `tests/review_probes.rs` holds every loop refusal to a typed code.

**Jobs.** `score.why`, `score.verify`, and `apply.receipt` run as jobs unless the call sets `wait: true` (a static rule, not a timer): the reply carries a job id, the BLAKE3 hash of the tool, its arguments (without `wait`), and the state the result depends on (the manifest sequence and the data's catalog digest, `Engine::state`), so a repeated start in the same state returns the same job. `jobs.status` reports progress; `jobs.result` returns the tool's reply once it has finished. A job id always names one result: calling the tool again directly with a finished job's request in the same state returns that job's stored result; after a commit or a data change the same request is a new job, so following a `stale_manifest` or `data_changed` next line literally makes progress; `wait: true` runs it again now.

`score.record` runs as a job by the same rule. An engine refusing an export is `export_denied`, with `fields.engine` and `fields.permission`; the measured Replay stands and no certificate or receipt is written.

**Budgets.** One server process is one session. Before any tool runs, a spent dimension refuses it with `budget_exceeded`; after it runs, the Replay records it wrote, the rows its verification read (both sides), and its wall time are charged (so `observe.*` is charged wall time too). A score or apply call that is not a dry run is admitted atomically: under the budget lock it is checked and reserves the replays and rows left; a concurrent one waits (its job's progress says so) until that call settles, its actual cost charged and the rest refunded, and is then checked. Concurrent jobs therefore behave as if run one at a time, and only the call that crosses a limit finishes over it. Bytes read are not a dimension: the facade reports what a plan would read, not what the engine read.

List keys (B4): `observe.locate` takes `contains: "any" | "all"` (= `locate --any` / `--all`) and `score.why` takes `contains` (= `why --mode contains-any|contains-all`); each tool's text is the CLI's `--json` line (`crates/bellwether-cli/tests/list.rs`).

## Resources and the prompt

`bellwether://findings` (the newest profile Evidence per root with its lints), `bellwether://receipts` (every receipt with its chain status), and `bellwether://scorecard` (per rule id: proposals, certificates passed and failed, receipts), each the facade's ledger view, byte for byte `bellwether ledger findings|receipts|scorecard --json`. One prompt, `diagnose_slow_query`, which is `AGENTS.md` (and the symptom the human described, when given).

## Threat model

|Surface|What it can do|Worst case|
|---|---|---|
|`observe.*`|read footers and the index; write the profile Evidence and cache entries|the agent reads evidence|
|`score.*`|ask the engine to replay and verify; the engine writes its own Proposals, Replays, plan Evidence, and Certificates; charged to the session budget; with `--twin-of`, submit a keys query or a rewrite's queries read-only to the one engine the human named|the agent spends its budget and is refused with `budget_exceeded`; a cost it tries to supply is refused with `caller_supplied_cost`|
|`propose.*`|write a `Proposal` with no score, or nothing with `dry_run`|the ledger holds a bad proposal that scores poorly and never gets a Certificate|
|`apply.receipt`|take a Certificate hash only; the engine applies and then writes the Receipt|the agent asks to apply something unverified and is told no, with a typed code|
|`jobs.*`|start a score or apply call as a job, poll it, read its result|whatever the started tool may do|
|resources|read the ledger|the agent reads the ledger|

## Tests

- `tests/threat_model.rs`: the loop end to end through the tools to a Receipt whose explain chain is intact; a score smuggled into every propose tool, top level or nested, is rejected and nothing is written; `apply.receipt` with a failing certificate, one against an older manifest, a proposal hash, and an absent hash each refused with its code and nothing written, dry run or not; refusal facts (reserved proposer, stale index file lists, no workload, budget); jobs. Every write-nothing test runs under a fixed clock and under the system clock.
- `tests/review_probes.rs`: the PR #5 review's probes, kept: eight concurrent jobs on a one-replay budget (one runs); the rows and seconds limits; every loop refusal typed, never `engine_error`, its next line naming the MCP field; `invalid_spec` with the column and the dataset's columns; dry runs never create an absent index location; the apply dry run and the apply agree on a key already built and write nothing; closed schemas; a finished job returned, not rerun; following the `stale_manifest` next line literally ends in a receipt.
- `tests/proptest_inputs.rs`: arbitrary JSON and mutations of valid arguments on every tool never panic and never write outside the tool's tier (the store's object list before and after each call).
- `tests/catalog.rs`: the tool list is the tiers; every schema closed with no dangerous field; the prompt is `AGENTS.md`; `AGENTS.md` documents every refusal code.
- `tests/stdio.rs`: the binary over a real stdio session: every stdout line is JSON-RPC, a tool's text over the wire is the in-process reply, schemas, prompt, and resources as published; a bad root exits 4 before the handshake; the binary takes every shared engine and budget flag.
- `tests/latency.rs`: `observe.why` under 100 ms in process and over a stdio round trip, asserted in CI's latency job on the bench's `replay-small` corpus (release build).
- `crates/bellwether-cli/tests/mcp.rs`: ADR 010, every tool with a CLI counterpart returns the CLI's `--json` line byte for byte, refusals included, and with non-default arguments (`columns`, `key_columns`, `max_candidates`, and the default cap where it binds); `bellwether mcp` takes the shared flags and serves over stdio.
- `tests/engine_tools.rs` (ADR 033, an in-process engine): `score.record`, `score.why` with gates, `propose.rewrite`, `score.verify`, and `apply.receipt` return the facade's JSON line, byte for byte where nothing is measured and up to a consistent renaming of run ids and record hashes where the engine measured; `caller_supplied_cost` for every cost-like field on every tool, direct and as a job, nested in a rewrite's shape and pairs, with nothing written and nothing submitted; `export_denied` naming the permission with no certificate and the prediction kept; `score.record` without `--twin-of` refused.
- `crates/bellwether-cli/tests/engine_tools.rs`: the same tools against the CLI binary and a real ClickHouse server (CI's service container).
- `tests/sql_root.rs`: `score.verify` refuses rewrite outputs that are `sql://` roots (a tool argument never names a database).
