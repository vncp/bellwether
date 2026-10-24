# Bellwether for coding agents

You are connected to a Bellwether MCP server bound to one data folder. Use
it to find out why a lookup over that folder reads more than it returns, to
propose a fix, and to have the engine prove and apply it. You propose; only
the engine scores, verifies, and applies (ADR 009). The MCP server sends you
this file as its one prompt, "diagnose a slow query", and the server's tests
fail if the two differ.

## Run the loop, in this order

1. **Observe.** Call `observe.why` with no keys. Read `evidence.hash` in the
   result: the profile Evidence, with fanout per column and lints. If you have
   the keys the slow query looked up (a Parquet file), call `score.why` with
   `keys`: the engine replays them under every candidate key and returns the
   funnel, the culprit, the recommended key, and `workload.hash`.
2. **Read the evidence.** Quote the lint (`rule`, columns, message) and the
   fanout that explain the read. Every number is an exact sum of footer
   statistics; none is an estimate.
3. **Propose.** Call `propose.key` (a key spec) or `propose.rewrite` (before,
   after, invariant: a description, never code), citing the evidence hash in
   `evidence` and your own name in `by`. Pass `dry_run: true` first if you
   want to see the Proposal without writing it. A proposal has no score
   field; anything that looks like one is rejected by the schema.
4. **Ask the engine to score and verify.** Call `score.verify` with the
   proposal hash (and `workload` for a key, the before and after outputs in
   `rewrite` for a rewrite). The engine replays the workload, reconciles the
   plan against a full scan, and writes a Certificate, passing or failing. A
   long run returns a job id; poll `jobs.status` and read `jobs.result`.
   Calling the same request again returns the same job, and once it has
   finished, its stored result; pass `wait: true` to run it again now.
5. **Apply.** Call `apply.receipt` with the certificate hash, after an
   optional `dry_run: true`, which runs the same checks and gives the same
   answer. The engine refuses unverified applies: a failing certificate, a
   certificate computed against an older manifest, data that changed since
   verification, or a key that is already complete is refused with a typed
   code before anything is written. On success it returns the Receipt.
6. **Report.** Quote the receipt hash to the human verbatim, with the
   predicted and realized bytes. `bellwether receipt show <hash>` (or the
   `explain` chain in the result) shows every record behind it.

Every result embeds the record hashes it rests on (`cites`); cite them, do
not paraphrase numbers. The resources `bellwether://findings`,
`bellwether://receipts`, and `bellwether://scorecard` list what the ledger
already holds.

## Tools

| Tier | Tool | Writes |
|---|---|---|
| observe | `observe.why` (no keys), `observe.profile`, `observe.locate`, `observe.asof` | Evidence and cache entries only |
| score (engine-scored) | `score.why` (with keys or a workload), `score.verify` | engine Proposals, Replays, plan Evidence, Certificates |
| propose | `propose.key`, `propose.layout`, `propose.rewrite` | one Proposal, or nothing with `dry_run` |
| apply | `apply.receipt` | Replay, the index commit, Receipt; nothing with `dry_run` or on a refusal, unless another commit lands during the apply (`concurrent_commit`: the key is built, the receipt withheld) |
| jobs | `jobs.start`, `jobs.status`, `jobs.result` | whatever the started tool writes |

## Refusal codes

Every refusal carries `code`, the facts you need to act, and one `next`
line. No refusal is prose only.

| Code | Meaning |
|---|---|
| `certificate_failed` | The certificate found differences; `fields` has the counts and the first mismatching keys. Do not apply; propose differently or ask the human. |
| `stale_manifest` | The certificate was computed against an older manifest. Call `score.verify` again. |
| `data_changed` | Files changed after verification. Call `score.verify` again. |
| `concurrent_commit` | Another commit landed during the apply; the key is built but not attested. Tell the human. |
| `nothing_to_apply` | The key is already complete; there is nothing to apply or attest. |
| `layout_not_applicable` | Layout proposals cannot be applied in this release. |
| `no_replay` | A rewrite verified but cannot be attested yet (rewrites have no replay). |
| `reserved_proposer` | The name in `by` is reserved for the engine. Pass your own name. |
| `no_workload` | Scoring needs input that was not given: `keys` or a recorded `workload` for `score.why`, `workload` for a key proposal, `rewrite` outputs for a rewrite; `fields.field` names it. |
| `stale_index` | Files landed, changed, or vanished since the index was built; `fields` lists them. Ask the human to run `bellwether index`. |
| `not_indexed` | The lookup needs an index the folder does not have (none at all, or no complete key spec of the kind it plans with, in `fields.needs`). Ask the human to index the folder. |
| `wrong_kind` | The hash is a record of another kind than the tool takes; `fields.field` names the argument. Pass the hash the earlier step printed. |
| `not_found` | The hash is not in this ledger (or, for a workload, the cache); `fields.kind` and `fields.field` say which argument. |
| `invalid_input` | The arguments do not match the tool's schema (an unknown field such as `score` included). |
| `invalid_argument` | An argument matches the schema but cannot be used (an empty `by`, no `evidence`, an `at` that is not an instant, an unreadable `keys` file); `fields.field` names it. |
| `invalid_spec` | The key spec or a column is not valid for this dataset. |
| `budget_exceeded` | This session spent its replay, rows, or wall-time budget. Stop and ask the human. Every tool call is charged its wall time; score and apply calls run one at a time against the budget. |
| `job_not_found` | No job has that id in this session. |
| `job_running` | The job has not finished; poll `jobs.status`. |
| `unknown_tool` | No tool has that name. |
| `source_error` | A data file could not be read. |
| `store_error` | The index store could not be read or written. |
| `ledger_rule` | The ledger refused a record or a link (for example a certificate that verified no proposal). |
| `version_conflict` | A concurrent writer won a commit race. Retry once. |
| `unsupported_version` | The index or a record is newer than this build. |
| `unknown_kind` | The index uses a kind this build does not know. |
| `segment_claimed` | A segment is being garbage-collected. Retry later. |
| `engine_error` | Any other engine error; `message` says what to do. |

## What you may never do

- Never write a Replay, a Certificate, or a Receipt yourself, and never
  supply a number the engine reports as measured. There is no field for it.
- Never pass a file path to write, a shell command, or a query string to
  execute; no tool accepts one. A rewrite is a description, never code.
- Never use the proposer name `engine`.
- Never report a fix as done without a receipt hash, and never present a
  failing or refused certificate as a pass.
- Never loop on a refusal: read `code` and `next`, and stop when the budget
  is spent.
