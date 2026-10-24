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

Flags: `--index <PATH|URL>` (default `<ROOT>/.bellwether`), `--format parquet|csv|jsonl`, and the session budget `--budget-replays N` (default 256), `--budget-rows N` (default 2,000,000,000), `--budget-seconds N` (default 3,600). `bellwether mcp` also takes the CLI's global flags (`--chunk-size`, `--schema`, `--fingerprint-policy`). Exit codes: 0 when the client disconnects; 2 usage; 4 the engine could not open the folder (before the handshake, nothing on stdout) or the transport failed.

## Tools

|Tier|Tool|Facade call|Writes|
|---|---|---|---|
|observe|`observe.why` (no keys)|`why` without a workload|the profile Evidence, cache entries|
|observe|`observe.profile`|`findings`|nothing|
|observe|`observe.locate`, `observe.asof`|`locate`, `asof`|the `plan` Evidence, cache entries|
|score|`score.why` (keys or a workload hash)|`why` with a workload|engine Proposals and Replays|
|score|`score.verify`|`fix` with `verify_only`|Replay, plan Evidence, Certificate|
|propose|`propose.key`, `propose.layout`, `propose.rewrite`|`propose`, or `propose_dry_run` with `dry_run`|one Proposal, or nothing|
|apply|`apply.receipt` (a certificate hash only)|`fix` with the certificate, or `apply_dry_run` with `dry_run`|the index commit, the realized Replay, the Receipt; nothing on a refusal or a dry run|
|jobs|`jobs.start`, `jobs.status`, `jobs.result`|the started tool's|the started tool's|

Every input schema is closed (`additionalProperties: false` at every depth), so a `score` field anywhere is `invalid_input`. No tool accepts a path to write, a shell command, or a query string to execute; the paths a tool accepts (a key file, a rewrite's output folders) are only read.

Each reply's text is the facade's typed result serialized exactly as the CLI's `--json` line prints it (ADR 010). The structured content is an envelope: the tool and tier, `status` (`ok`, `refused`, `running`), `cites` (every record hash in the result, with its kind), `chains` (the explain chain of every receipt the result mentions, and whether it is intact), `refusal` (a stable `code`, the facts to act on, one `next` line), `job`, and the session `budget`.

**Jobs.** `score.why`, `score.verify`, and `apply.receipt` run as jobs unless the call sets `wait: true` (a static rule, not a timer): the reply carries a job id, the BLAKE3 hash of the tool and its arguments (without `wait`), so a repeated start returns the same job. `jobs.status` reports progress; `jobs.result` returns the tool's reply once it has finished.

**Budgets.** One server process is one session. Before a tool runs, a spent dimension refuses it with `budget_exceeded`; after it runs, the Replay records it wrote, the rows its verification read (both sides), and its wall time are charged. Bytes read are not a dimension: the facade reports what a plan would read, not what the engine read.

## Resources and the prompt

`bellwether://findings` (the newest profile Evidence per root with its lints), `bellwether://receipts` (every receipt with its chain status), and `bellwether://scorecard` (per rule id: proposals, certificates passed and failed, receipts), each the facade's ledger view, byte for byte `bellwether ledger findings|receipts|scorecard --json`. One prompt, `diagnose_slow_query`, which is `AGENTS.md` (and the symptom the human described, when given).

## Threat model

|Surface|What it can do|Worst case|
|---|---|---|
|`observe.*`|read footers and the index; write the profile Evidence and cache entries|the agent reads evidence|
|`score.*`|ask the engine to replay and verify; the engine writes its own Proposals, Replays, plan Evidence, and Certificates; charged to the session budget|the agent spends its budget and is refused with `budget_exceeded`|
|`propose.*`|write a `Proposal` with no score, or nothing with `dry_run`|the ledger holds a bad proposal that scores poorly and never gets a Certificate|
|`apply.receipt`|take a Certificate hash only; the engine applies and then writes the Receipt|the agent asks to apply something unverified and is told no, with a typed code|
|`jobs.*`|start a score or apply call as a job, poll it, read its result|whatever the started tool may do|
|resources|read the ledger|the agent reads the ledger|

## Tests

- `tests/threat_model.rs`: the loop end to end through the tools to a Receipt whose explain chain is intact; a score smuggled into every propose tool, top level or nested, is rejected and nothing is written; `apply.receipt` with a failing certificate, one against an older manifest, a proposal hash, and an absent hash each refused with its code and nothing written, dry run or not; refusal facts (reserved proposer, stale index file lists, no workload, budget); jobs.
- `tests/proptest_inputs.rs`: arbitrary JSON and mutations of valid arguments on every tool never panic and never write outside the tool's tier (the store's object list before and after each call).
- `tests/catalog.rs`: the tool list is the tiers; every schema closed with no dangerous field; the prompt is `AGENTS.md`; `AGENTS.md` documents every refusal code.
- `tests/stdio.rs`: the binary over a real stdio session: every stdout line is JSON-RPC, a tool's text over the wire is the in-process reply, schemas, prompt, and resources as published; a bad root exits 4 before the handshake.
- `tests/latency.rs`: `observe.why` under 100 ms in process and over a stdio round trip, asserted in CI's latency job on the bench's `replay-small` corpus (release build).
- `crates/bellwether-cli/tests/mcp.rs`: ADR 010, every tool with a CLI counterpart returns the CLI's `--json` line byte for byte, refusals included; `bellwether mcp` serves over stdio.
