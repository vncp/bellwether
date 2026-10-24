# Threat model for the agent surface

**Claim:** a compromised, confused, or hallucinating agent can waste your time by proposing bad ideas, and can do nothing else.

| Surface | What it can do | Worst case |
|---|---|---|
| `observe.*` (`observe.why` with no keys, `observe.profile`, `observe.locate`, `observe.asof`) | read footers and the index; write the profile Evidence and cache entries | the agent reads evidence |
| `score.*` (`score.why` with keys or a workload, `score.verify`) | ask the engine to replay and verify; the engine writes its own Proposals, Replays, plan Evidence, and Certificates, each computed immediately before it is written; charged to the session budget | the agent spends its budget and is refused with `budget_exceeded` |
| `propose.*` | write a `Proposal` with no score, or nothing with `dry_run` | the ledger holds a bad proposal that scores poorly and never gets a Certificate |
| `apply.receipt` | take a Certificate hash only; the engine applies and then writes the Receipt | the agent asks to apply something unverified and is told no, with a typed code |
| `jobs.*` | start a score or apply call as a job, poll it, read its result | whatever the started tool may do |
| resources (`bellwether://findings`, `receipts`, `scorecard`) | read the ledger | the agent reads the ledger |

`observe.why` and `score.why` are the same facade call (`why`) split by cost. Without keys it reads footers and records the profile Evidence: read-only on the data, cheap, never charged. With keys (a Parquet file) or a recorded workload hash it replays the lookup under every candidate key, which can take minutes and writes engine-scored records, so it sits in the score tier, runs as a job unless the agent asks to wait, and is charged to the budget. `observe.why` has no `keys` field; passing one is `invalid_input`.

Rules that make the claim true:

- No tool accepts a file path to write, a shell command, or a query string to execute. Rewrites are data (`RewriteShape`: before, after, invariant), never code the engine runs. The paths a tool does accept (a key file, a rewrite's before and after output folders) are only read.
- Every tool's input schema is generated from the contract's types (the `schema` feature of `bellwether-types`) and closed: every object, nested ones included, has `additionalProperties: false`. A `score`, `confidence`, or `replay` field anywhere in a propose call is `invalid_input`, and nothing is written.
- The engine never calls the model. The model calls the engine. The direction of dependency is the security property.
- `apply.receipt` resolves the Certificate and refuses unless `pass == true` and the certificate was computed against the current manifest version, and unless the files are those the certificate verified. Only after the apply succeeds does the engine write the Receipt, recording `manifest_before` (the version the certificate was computed against) and `manifest_after` (the version the apply produced). A hash that names another kind of record is refused as `wrong_kind`.
- `Proposal` has no score field. Scoring cannot be injected because there is nowhere to put it. The proposer name `engine` is reserved (`reserved_proposer`).
- Every refusal carries a stable `code`, the facts needed to act (the mismatch counts and first mismatching keys of a failed certificate, the untracked, changed, and missing files of a stale index), and one `next` line. `AGENTS.md` lists every code; the test suite fails if a code is missing from it.
- `dry_run: true` on `propose.*` and `apply.receipt` runs every check and returns what would be written or refused, and writes nothing anywhere (tests compare the store's object list before and after).
- Each server process is one session with a budget (`--budget-replays`, `--budget-rows`, `--budget-seconds`): Replay records written, rows the verifier read, and wall time of tool calls. A spent dimension refuses every further call with `budget_exceeded` before it runs. The facade's operations are not cancellable, so one call that starts under budget may finish over it.
- Every result's envelope lists the record hashes it rests on (`cites`) and, for every receipt it mentions, the receipt's explain chain and whether it is intact.
- Every Evidence and Certificate carries provenance: inputs, age, creation time, and the manifest version when one exists (always for a Certificate; an Evidence about an unindexed folder has none yet). An agent cannot present stale numbers as fresh; the age is on the record.
- The server reads nothing from the environment, logs to stderr only, and opens no network connection beyond the store the facade opens.

Tests: `crates/bellwether-mcp/tests/threat_model.rs` (the loop end to end, smuggled scores, every apply refusal writing nothing, refusal facts, budgets, jobs), `proptest_inputs.rs` (arbitrary and mutated JSON on every tool never panics and never writes outside the tool's tier), `catalog.rs` (closed schemas, the prompt is `AGENTS.md`), `stdio.rs` (the binary over a real stdio session), and `crates/bellwether-cli/tests/mcp.rs` (ADR 010: every tool's text is the CLI's `--json` line).

Out of scope: an attacker with write access to the store. Content addressing makes tampering detectable but not preventable; signing is a later addition.
