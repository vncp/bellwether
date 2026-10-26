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
   funnel, the culprit, the recommended key, and `workload.hash`. When the
   server was started with `--twin-of` (the folder is the lake copy, the
   *twin*, of tables a query engine stores), call `score.record` with a
   `keys_query` in the engine's language to record the keys as a workload,
   then `score.why` with that `workload` and any `gates` (`{"name": "in",
   "limit": 1000}`): the report says the diagnosis is on a copy, records
   the engine's table identity beside the lake's sequence, gives per
   candidate the keyset against each gate, rows per key, and files per key,
   and adds the twin lints (advice that names a rewrite pattern).
2. **Read the evidence.** Quote the lint (`rule`, columns, message) and the
   fanout that explain the read. Every number is an exact sum of footer
   statistics; none is an estimate, except those labelled `approximate`:
   the `sketch` column, the `R-sketch-1` lint, `observe.cardinality`, and
   `observe.overlap` read `sketch` sidecars (distinct counts with an error
   bound) and never decide a plan. To ask whether a column is a key without
   reading the data, call `observe.cardinality`, or `observe.why` with
   `sketches_only: true`.
3. **Propose.** Call `propose.key` (a key spec, as JSON or in the short form
   such as `"exact:id"`) or `propose.rewrite` (before, after, invariant: a
   description, never code; for a query engine, `shape.pairs` holds one or
   more `{before, after}` query texts you wrote in the engine's language,
   scored and verified as one set, and the proposal must cite the twin
   Evidence), citing the evidence hash in
   `evidence` and your own name in `by`. Pass `dry_run: true` first if you
   want to see the Proposal without writing it. A proposal has no score
   field; anything that looks like one is rejected by the schema.
4. **Ask the engine to score and verify.** Call `score.verify` with the
   proposal hash (and `workload` for a key, the before and after outputs in
   `rewrite` for a rewrite). The engine replays the workload, reconciles the
   plan against a full scan, and writes a Certificate, passing or failing. A
   long run returns a job id; poll `jobs.status` and read `jobs.result`.
   For a rewrite of engine queries the engine submits every query under
   its own tag, read-only, reads each one's statistics from the engine's
   own query log (`measured`, never `exact`), writes that Replay with the
   twin's prediction beside it first, then exports each pair's results to
   the operator's scratch folder and reconciles them by the proposal's key
   columns (`engine` sets latest-by and totals). You never supply a number:
   any `score`, `cost`, `replay`, `stats`, or `verdict` field is refused
   with `caller_supplied_cost`.
   Calling the same request again in the same state returns the same job,
   and once it has finished, its stored result; after a commit or a data
   change it runs again. Pass `wait: true` to run it again now.
5. **Apply.** Call `apply.receipt` with the certificate hash, after an
   optional `dry_run: true`, which runs the same checks and gives the same
   answer. The engine refuses unverified applies: a failing certificate, a
   certificate computed against an older manifest, data that changed since
   verification, or a key that is already complete is refused with a typed
   code before anything is written. On success it returns the Receipt. For
   a rewrite of engine queries the engine measures the queries again and
   writes an attest-only Receipt (`attest_only: true`, the manifest
   unchanged): nothing is deployed; a human or a CI gate deploys the
   `after` queries.
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
| observe | `observe.why` (no keys), `observe.cardinality`, `observe.overlap`, `observe.profile`, `observe.locate`, `observe.asof` | Evidence and cache entries only |
| score (engine-scored) | `score.why` (with keys or a workload), `score.verify`, `score.record` (keys from the query engine, with `--twin-of`) | engine Proposals, Replays, plan Evidence, Certificates; `score.record` a workload in the cache |
| propose | `propose.key`, `propose.layout`, `propose.rewrite` | one Proposal, or nothing with `dry_run` |
| apply | `apply.receipt` | Replay, the index commit, Receipt; nothing with `dry_run` or on a refusal, unless another commit lands during the apply (`concurrent_commit`: the key is built, the receipt withheld) |
| jobs | `jobs.start`, `jobs.status`, `jobs.result` | whatever the started tool writes |

## Signatures in `chains[]`

Each receipt chain in a result's envelope lists `signatures`: one entry per
signature object beside the receipt or its certificate, with `status`
`valid` (it verifies under its own public key) or `invalid` (with a
`reason`). When the server runs with `--trusted-keys`, the chain also has a
`verdict`:

- `signed`: every link is present, the receipt and its certificate are each
  signed by a trusted key within its window, and the evidence is about this
  server's data folder.
- `links_flagged`: a link is missing, of the wrong kind, or unreadable.
- `invalid`: a signature does not verify.
- `untrusted`: signed, but by no trusted key within its window.
- `unsigned`: no signature on the receipt or its certificate.
- `other_root`: validly signed, but for another data folder (a chain copied
  from another index); it vouches for nothing here.
- `no_root`: signed, but no evidence in the chain names a data folder.

Without `--trusted-keys` there is no verdict. You cannot sign anything: the
engine signs only what `score.verify` and `apply.receipt` wrote, and only
when the server was started with `--signing-key`. Quote the verdict as
given; never call a chain signed unless its verdict is `signed`.

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
| `nothing_to_gain` | The proposal's replay predicts no saving on the workload; `fields` has `replay`, `current_bytes`, `proposed_bytes`. Nothing but the Replay was written. Propose another key; only the human can continue, with `bellwether fix --allow-no-gain` (no tool takes it). |
| `layout_not_applicable` | Layout proposals cannot be applied in this release. |
| `no_replay` | A rewrite verified but cannot be attested yet (rewrites have no replay). |
| `attest_only` | The proposal verified and its target is a table (`sql://`): the engine applies nothing to a table and writes no receipt. For a key, `fields.run` lists the `CREATE INDEX` per table the engine wrote, for a human to run; for a rewrite, `fields.proposed_rewrite` is your own proposed text, which the engine never ran, for the human to review. Report the certificate and those fields to the human. The same gain gate as over files applies first (`nothing_to_gain`). |
| `reserved_proposer` | The name in `by` is reserved for the engine. Pass your own name. |
| `sidecar_not_proposable` | The key in `spec` declares a sidecar (`temporal2:id@valid,known`, or `exact:a@ts` with per-key time bounds), which a proposal cannot carry yet, so it has no replay, certificate, or receipt. `fields` has `declaration`, `sidecar`, `key` (the key spec it rides beside, or null), `root`, and `command` (the `bellwether index` command that builds it, shell-quoted, with this server's source flags; null when no command line spells them). Propose `fields.key` alone to reach a receipt, or ask the human to run `fields.command`. |
| `no_workload` | Scoring needs input that was not given: `keys` or a recorded `workload` for `score.why`, `workload` for a key proposal, `rewrite` outputs for a rewrite; `fields.field` names it. |
| `stale_index` | Files landed, changed, or vanished since the index was built; `fields` lists them. Ask the human to run `bellwether index`. |
| `not_indexed` | The lookup needs an index the folder does not have (none at all, or no complete key spec of the kind it plans with, in `fields.needs`). Ask the human to index the folder. |
| `not_retained` | The lookup was pinned with `at_manifest` to a sequence the index does not retain (never committed, or pruned by garbage collection); `fields.oldest` and `fields.current` bound the sequences you may pass (both null when the folder is not indexed). Pass one of them, or omit `at_manifest`. |
| `wrong_kind` | The hash is a record of another kind than the tool takes; `fields.field` names the argument. Pass the hash the earlier step printed. |
| `not_found` | The hash is not in this ledger (or, for a workload, the cache); `fields.kind` and `fields.field` say which argument. |
| `invalid_input` | The arguments do not match the tool's schema (an unknown field included; a score, cost, or verdict field is `caller_supplied_cost`). |
| `invalid_argument` | An argument matches the schema but cannot be used (an empty `by`, no `evidence`, an `at` that is not an instant, an unreadable `keys` file); `fields.field` names it. |
| `invalid_spec` | The key spec or a column is not valid for this dataset. |
| `budget_exceeded` | This session spent its replay, rows, or wall-time budget. Stop and ask the human. Every tool call is charged its wall time; score and apply calls run one at a time against the budget. |
| `job_not_found` | No job has that id in this session. |
| `job_running` | The job has not finished; poll `jobs.status`. |
| `unknown_tool` | No tool has that name. |
| `source_error` | A data file or table could not be read. |
| `source_changed` | A table changed during a run that held no snapshot (`fields.units` lists the units); nothing was recorded. Call the tool again when the table is quiet. |
| `not_a_folder` | The data root is a table and the tool needs a folder of files (lookups, index builds). Over a table use `observe.why`, `observe.profile`, `score.why`, and `score.verify`. |
| `store_error` | The index store could not be read or written. |
| `ledger_rule` | The ledger refused a record or a link (for example a certificate that verified no proposal). |
| `version_conflict` | A concurrent writer won a commit race. Retry once. |
| `unsupported_version` | The index or a record is newer than this build. |
| `unknown_kind` | The index uses a kind this build does not know. |
| `segment_claimed` | A segment is being garbage-collected. Retry later. |
| `engine_error` | Any other engine error; `message` says what to do. |
| `export_denied` | The query engine refused to export a rewrite's results, or the scratch folder cannot be written; `fields.engine` and `fields.permission` name what to grant. The export is the measured run, so nothing was measured: no Replay, no certificate, no receipt. Tell the human which permission. |
| `caller_supplied_cost` | The arguments carried a score, cost, measurement, replay, prediction, verdict, or certificate field (`fields.field`). The engine measures, verifies, and attests on its own; remove the field and call again. |
| `measurement_refused` | A query engine's log entry for one run cannot stand as its measurement; `fields.reason` says why: `cache_served` (the query text turned a cache on), `statistics_missing`, `not_the_query_submitted` or `ambiguous_log_entry` (another client ran under the run's tag), `tag_mismatch`, or `result_rows_differ` (the rows the run returned are not the rows it exported). Nothing was recorded. For `cache_served`, propose the rewrite again without those settings. |
| `measurement_changed` | `apply.receipt` measured a rewrite of engine queries again, in the state certified, and a query's rows read, bytes read, or result rows disagree with the certified replay's (`fields.pair`, `fields.side`, `fields.certified`, `fields.measured`). Nothing was written. Do not apply; verify the proposal again. |

## What you may never do

- Never write a Replay, a Certificate, or a Receipt yourself, and never
  supply a number the engine reports as measured. There is no field for it.
- Never pass a file path to write or a shell command; no tool accepts one.
  A rewrite of lake outputs is a description, never code. The only query
  text a tool takes is a `keys_query` and a rewrite's `shape.pairs`: the
  engine submits it read-only, under its own tags, to the one query engine
  the human started the server with (`--twin-of`), never to the lake or a
  shell; no argument names an engine or a folder.
- Never use the proposer name `engine`.
- Never report a fix as done without a receipt hash, and never present a
  failing or refused certificate as a pass.
- Never loop on a refusal: read `code` and `next`, and stop when the budget
  is spent.
