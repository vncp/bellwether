# The agent loop

This page explains, for a reader rather than an implementer, how a finding
becomes a fix. It follows one example from the first observation to the
receipt and shows every record the engine writes on the way. The records
are the types in
[`crates/bellwether-types/src/lib.rs`](../crates/bellwether-types/src/lib.rs);
the boundary they enforce is in [`architecture.md`](architecture.md) and
[`threat-model.md`](threat-model.md).

Status: pre-alpha. The engine writes every record this page shows, through
the CLI (`bellwether why`, `propose`, `fix`, `receipt show`). The example is
illustrative and every value in angle brackets is a placeholder.

## The four stages

> The agent may **observe** and **propose**. Only the engine may
> **measure**, **verify**, and **apply**.

| Stage | Who acts | Record written | What it cannot do |
|---|---|---|---|
| 1 Observe | agent or human | `Evidence` | write anything else |
| 2 Propose | agent or human | `Proposal` | carry a score; there is no field for one |
| 3 Score and verify | engine only | `Replay`, then `Certificate` | be skipped |
| 4 Apply and attest | engine only | `Receipt` | happen without a passing `Certificate` |

A human reaches stages 1 and 2 through the CLI. An agent reaches the same
code through the CLI with `--json` today, and through the `observe.*` and
`propose.*` MCP tools when WP12 lands (ADR 010). Stages 3 and 4 run inside
the engine. The agent can ask for them, but it cannot supply their
results.

## How records are stored

Every record is stored in the ledger as canonical JSON, named by the BLAKE3
hash of that JSON ([`format.md`](format.md), ADR 006). On disk each record
is wrapped in the `Record` enum, which adds a top-level `"record"` field
naming the kind. The shapes below include that field.

Details that matter when reading or producing these records:

- Hashes are 64-character lowercase hex strings.
- Fields appear in the order the Rust struct declares them. That order is
  part of the hash: the same values in a different order hash differently.
- Tagged enums use a `"kind"` field (`EvidenceBody`, `ProposalBody`,
  `KeySpec`, `Coverage`) or an `"op"` field (`Predicate`).
- `oldest_input_age` is a whole number of nanoseconds (contract v2).
- `created_at` is RFC 3339 UTC with millisecond precision
  (`2026-10-24T21:03:02.069Z`), written by the facade's one clock for every
  record kind.
- `record_version` is the writing build's `bellwether_types::format::RECORD_VERSION`: 4 since tombstones (B8, ADR 028), whose key specs and live filters a version 3 reader would misread, so it refuses a version 4 record. The shapes below show 2 (contract v2.1), as written then; every earlier version remains readable, its bytes unchanged.

The blocks below are shapes, not parseable JSON: angle-bracket placeholders
stand in for values.

## Worked example

A nightly job looks up the orders named in a file of trigger keys,
`trigger_keys.parquet`, in a folder of order files, `./orders/`. It looks
them up by `order_id`. The only declared key on the folder is `batch_id`,
which looks like an id but is stamped per write batch, so it cannot narrow
an `order_id` lookup. The job reads `<B_now>` to return `<N rows>` rows.
The question is why.

### 1. Observe: Evidence

The agent calls `observe.why` (or a human runs `bellwether why`). The engine
reads only footers and row-group statistics, measures fanout per column and
per candidate column tuple, runs the lints in
[`rule-catalog.md`](rule-catalog.md), and summarizes the recorded workload.
It writes one `Evidence` of kind `profile`.

```text
{
  "record": "evidence",
  "record_version": 2,
  "provenance": {
    "inputs": ["<hash of footer cache>", "<hash of recorded workload>"],
    "oldest_input_age": <nanoseconds>,
    "manifest_version": "<manifest version, or null before any index>",
    "created_at": "<RFC 3339>"
  },
  "body": {
    "kind": "profile",
    "root": "./orders/",
    "fanout": [
      {
        "columns": ["batch_id"],
        "distinct": <N distinct>,
        "sampled": false,
        "files_per_value": [<p50>, <p95>, <max>],
        "row_groups_per_value": [<p50>, <p95>, <max>],
        "files_total": <N files>,
        "row_groups_total": <R row groups>
      },
      {
        "columns": ["order_id"],
        "distinct": <N distinct>,
        "sampled": false,
        "files_per_value": [<p50>, <p95>, <max>],
        "row_groups_per_value": [<p50>, <p95>, <max>],
        "files_total": <N files>,
        "row_groups_total": <R row groups>
      }
    ],
    "lints": [
      {
        "rule": "batch_stamp_key",
        "columns": ["batch_id"],
        "message": "<one sentence a non-specialist can act on>"
      }
    ],
    "workload_keys": [<p50 keys>, <p95 keys>, <max keys>]
  }
}
```

`provenance` is mandatory on Evidence (invariant 6). It says what the
numbers were computed from and how old the oldest input was, so an agent
cannot present a stale measurement as fresh. `sampled` says whether the
fanout came from a sample of distinct values. `workload_keys` is `null` when
no workload was recorded.

The other Evidence kind, `plan`, is written by `locate`, `latest`, and
`asof`. It carries a `ScanPlan`:

```text
{
  "record": "evidence",
  "record_version": 2,
  "provenance": { ... },
  "body": {
    "kind": "plan",
    "plan": {
      "manifest_version": "<manifest version>",
      "entries": [
        {
          "file": {
            "path": "<path>",
            "size": <bytes>,
            "etag": "<etag or mtime, or null>",
            "tail_blake3": "<hash>"
          },
          "row_groups": [<roaring portable bytes, as an array of u8>],
          "bytes": <bytes>
        }
      ],
      "residual": { "op": "in", "columns": ["order_id"],
                    "values": [[<canonical bytes>], ...] },
      "bytes": <bytes>,
      "row_groups_total": <R row groups>,
      "row_groups_selected": <r selected>
    }
  }
}
```

`row_groups` is the roaring bitmap's portable serialization, not a list of
row-group indices; decode it with a roaring library. `values` holds
canonical-encoded key tuples ([`format.md`](format.md)) as byte arrays.

### 2. Propose: Proposal

The agent reads the Evidence and sees that `batch_id` touches many files per
value while `order_id` touches few. It proposes an exact index on
`order_id`, citing the Evidence by hash.

```text
{
  "record": "proposal",
  "record_version": 2,
  "evidence": ["<evidence hash>"],
  "proposed_by": "<agent name, user, or \"engine\">",
  "created_at": "<RFC 3339>",
  "body": {
    "kind": "key",
    "spec": { "kind": "exact", "columns": ["order_id"] }
  }
}
```

There is no score, no estimate, and no confidence field. The agent may say
anything; nothing it says is trusted. The other proposal kinds have these
bodies:

```text
{ "kind": "layout", "cluster_by": ["<column>", ...] }

{ "kind": "rewrite",
  "shape": { "before": "<current shape>",
             "after": "<target shape>",
             "invariant": "<what must be preserved>" } }
```

A rewrite is a description, never code. The engine does not execute it
(threat model). The `KeySpec` kinds are `exact` and `bloom` (with `columns`;
`bloom` adds `fpr`), `range` (with a single `column`), and `temporal` (with
`columns` and `ts_column`).

### 3a. Score: Replay

The engine replays the recorded workload: for each recorded run it computes
the files, row groups, and bytes the run would read under the current
configuration and under the proposal. The numbers are exact sums from
row-group statistics, not estimates (ADR 005), and the same inputs always
produce a byte-identical Replay (invariant 3).

```text
{
  "record": "replay",
  "record_version": 2,
  "proposal": "<proposal hash>",
  "manifest_version": "<manifest version>",
  "workload": "<hash of recorded workload>",
  "inputs": ["<catalog digest over the discovered fingerprints and footers>"],
  "current": [
    { "run_id": "<run>", "keys": <K>, "files": <F_now>,
      "row_groups": <R_now>, "bytes": <B_now> }
  ],
  "proposed": [
    { "run_id": "<run>", "keys": <K>, "files": <F_fix>,
      "row_groups": <R_fix>, "bytes": <B_fix> }
  ],
  "model": <absent for a key proposal; for a layout proposal, the name of the cost model behind the proposed costs>,
  "unserved": <absent, or "parquet_stats" when a run no key spec serves was priced as the files' own min/max statistics and bloom filters read it>,
  "created_at": "<RFC 3339>"
}
```

`inputs` is what makes two replays of one proposal distinguishable when the
folder has no index yet and `manifest_version` is `"0"`: the catalog digest
changes whenever a file lands or changes (invariant 3). The ledger refuses a
replay with no inputs, a replay whose current costs are not exact, a replay
that names a model although every cost is exact or omits one although a
proposed cost is inexact, and a key-proposal replay that is not exact in
every run.

Over Parquet files, a run that no key spec of a configuration serves is
priced as the files' own footer statistics read it (what DuckDB or any
Parquet reader skips to without an index), not as a full scan, and the
replay says so in `unserved`. `why`'s numbers, the score `fix` gates on, and
the realized cost after an apply are all this one measure.

A Replay says how much less the fix would read. It does not say the fix is
correct. That is the Certificate's job.

### 3b. Verify: Certificate

The engine builds the proposed index, runs the job both ways, and
reconciles the outputs by key: latest version per key, a row hash over the
canonical encoding, and exact decimal totals per group (ADR 004, ADR 016).

```text
{
  "record": "certificate",
  "record_version": 2,
  "proposal": "<proposal hash, or null for a standalone reconcile>",
  "coverage": {
    "kind": "workload",
    "workload": "<hash of recorded workload>",
    "keys": <K keys>
  },
  "pass": true,
  "body": {
    "missing_right": 0,
    "missing_left": 0,
    "mismatched": 0,
    "ignored_columns": ["<column excluded from the row hash>"],
    "totals_delta": []
  },
  "provenance": { ... }
}
```

`pass` is true only when every count is zero and `totals_delta` is empty.
When totals differ, each entry is a three-element array
`["<group>", "<left total>", "<right total>"]`, decimals rendered as strings
at declared scale. `ignored_columns` is listed so a reader can see what the
comparison did not check.

`coverage` says how much of the key space was compared:

```text
{ "kind": "full" }
{ "kind": "workload", "workload": "<hash>", "keys": <K> }
{ "kind": "sampled", "keys": <K>, "fraction": <0..1> }
```

A Certificate is a statement about the keys it covers and nothing more.

### 4. Apply and attest: Receipt

With a passing Certificate in hand, the engine applies the proposal and
writes a Receipt that links the chain. The Receipt records the manifest
version before and after the apply.

```text
{
  "record": "receipt",
  "record_version": 2,
  "evidence": ["<evidence hash>"],
  "proposal": "<proposal hash>",
  "replay": "<replay hash>",
  "certificate": "<certificate hash>",
  "manifest_before": "<manifest version before apply>",
  "manifest_after": "<manifest version after apply>",
  "created_at": "<RFC 3339>"
}
```

Stage 4 is apply, then attest. The engine applies a verified proposal, which
advances the manifest, and only then writes the Receipt. Manifest versions
are sequence numbers (ADR 020), compared numerically. `manifest_before` is the
sequence the Certificate and the Replay were computed against.
`manifest_after` is the sequence the apply produced. Maintenance commits
(GC, compaction) also advance the sequence and may land between the apply and
the Receipt write, so `manifest_after` is no later than the current sequence
rather than equal to it.

The ledger refuses to store a Receipt (invariant 2) if any of these holds:

- its certificate does not resolve, did not pass, or reports differences
  despite passing;
- its certificate did not verify exactly this proposal;
- its certificate or its replay was not computed against `manifest_before`;
- its replay scored a different proposal;
- its `manifest_after` is not greater than `manifest_before` (only a Rewrite
  may leave the manifest unchanged), or is greater than the current sequence.

A Receipt therefore cannot be forged from an unverified proposal, cannot cite
a verification made against another manifest, and cannot claim an apply
whose commit is not yet current. The full rule list is in
[`crates/bellwether-ledger/README.md`](../crates/bellwether-ledger/README.md).

`bellwether receipt show <receipt hash>` walks back from the Receipt through
the Certificate, Replay, Proposal, and Evidence, so a reviewer or a CI gate
reads one record and can check every step behind it.

## Second worked example: a query engine that owns its storage

The same loop runs when the slow query lives in a query engine that keeps
its own storage (ClickHouse, Kusto) and a Parquet copy of the same tables,
the lake *twin*, sits beside it (ADR 033). The diagnosis is about the
data's shape, so it runs on the twin; the measurement is about the engine,
so the engine measures it. Nothing the agent reports is recorded as a cost.

Every command runs on the lake folder. `--twin-of` names the engine; a
secret is read from the environment only (`password=env:NAME`).

```text
ENGINE='clickhouse://reader@ch.internal:8123/sales/orders?password=env:CH_PASSWORD'
```

### 1. Keys from the engine: a workload

```text
$ bellwether workload record ./orders --from-engine "$ENGINE" --keys-query keys.sql
recorded workload <workload>: 603 keys over (id, batch) from 603 rows of clickhouse://ch.internal:8123/sales/orders (query bw-<run>-keys)
```

The keys query runs in the engine, read-only, under a tag bellwether
chose; its rows are exported as Parquet into the scratch folder (or a
temporary one), recorded as a workload over the twin's types, and deleted.

### 2. Diagnose on the twin: Evidence with the engine's identity

```text
$ bellwether why ./orders --workload <workload> --twin-of "$ENGINE" --gate in:100 --gate join:100000
why ./orders: not indexed
  diagnosis on the lake twin of clickhouse://ch.internal:8123/sales/orders: every count is exact for the lake copy, and the engine stores its tables differently, so its own costs are measured by fix, never derived from these
  freshness: the engine holds 600,000 rows in 4 units (identity <id>); the lake at sequence 0 holds 600,000 rows: the same row count
...
twin: 603 sampled keys hold 603 rows in 24 row groups and 4 files of the lake (the prediction fix compares the engine with); ...
  candidate                              keyset  rows/key p50/p95/max     files/key          gates (in<=100, join<=100,000)
  the sample's key                          603  1 / 1 / 1                1 / 1 / 1          in OVER, join fits
  exact:id                                  201  3 / 3 / 3                1 / 1 / 1          in OVER, join fits
  exact:batch                                 6  100,000 / 100,000 / 100,000 2 / 2 / 2      in fits, join fits
  twin lints (advice; they change no plan)
    unbounded_membership [id, batch]
      the key sample holds 603 distinct keys, above the membership gate of 100: ...
```

The profile Evidence records `twin`: the engine root (no credentials), its
table identity, unit and row counts, the lake's sequence and rows, the
workload, and the rows, row groups, and files of the twin that hold a
sampled key: the twin's prediction. The four twin lints of the rule
catalog fire from these numbers and name a rewrite pattern; they change no
plan.

### 3. The agent rewrites the query: a Proposal

The agent writes the rewrite in the engine's language; bellwether never
writes or parses it.

```text
$ bellwether propose ./orders --rewrite before.sql=after.sql --evidence <twin evidence> --by agent
proposal <proposal>
```

The Proposal's `RewriteShape` carries the pairs (`pairs`, any number,
scored and verified as one set), the key columns their results are
compared by (`key`, default the sample's), and the key sample (`keys`). It
has no score field. A proposal that cites no twin Evidence is refused.

### 4. The engine scores, exports, reconciles, and attests

```text
$ bellwether fix ./orders --twin-of "$ENGINE" --proposal <proposal> --scratch ./orders/_scratch
fix <proposal>: rewrite of 1 engine query, compared by (id, batch), against manifest sequence 0
  1 score   in clickhouse://ch.internal:8123/sales/orders, measured, not exact (clickhouse.query_log): replay <replay>
                        rows read     bytes read    peak memory       cpu ms     result
    p0 before        600,000        16.8 MB         4.1 MB           14        603  bw-<run>-p0-before
    p0 after         600,000        12.0 MB         4.0 MB           10        603  bw-<run>-p0-after
            twin predicted 603 rows in 24 row groups; the engine read 600,000 rows before (ratio 995.025) and 600,000 after (ratio 995.025)
            engine identity <id> (4 units, 600,000 rows), unchanged through the exports
            exports: 6.0 kB under ./orders/_scratch/bellwether-<run>, deleted after the certificate
  2 verify  pass, coverage full, 0 mismatched, 0 missing left, 0 missing right, certificate <certificate>
  3 apply   attest-only: nothing applied in clickhouse://ch.internal:8123/sales/orders; a human or a CI gate deploys the rewritten query
  4 attest  receipt <receipt> (attest-only)
  deploy p0: <the after query>
```

- **Score.** Every query of every pair is submitted with a tag bellwether
  chose, read-only, and its statistics are read from the engine's own log
  by that tag. The Replay holds them (`exact: false`, `model:
  clickhouse.query_log`, `RunCost.measured`) and the prediction: the
  twin's rows beside the rows the engine read, and their ratio. It is
  written before anything else, so a failing certificate keeps it.
- **Verify.** Each pair's results are exported as Parquet into a fresh
  folder of the scratch folder and reconciled by the proposal's key
  columns. One Certificate covers every pair and cites the engine's
  identity before scoring and after the exports; a change between them is
  refused (`source_changed`). Without the right to export there is no
  certificate and no receipt: `export_denied` names the permission.
- **Attest.** The Receipt is marked `attest_only` and leaves the manifest
  where it was. It binds the twin Evidence, the Proposal (the query texts,
  hashed with it), the measured Replay, the Certificate, and the
  prediction. A human or a CI gate deploys the `after` queries; bellwether
  changes nothing in the engine.

`bellwether receipt show` prints the chain with `apply=attest-only`, the
model, `costs=measured, not exact`, and the prediction; `bellwether report
--html` marks the costs "measured, not exact" with the model's name and
lists the prediction error per receipt, which says, over receipts, how far
the twin can be trusted for that engine.

## What the agent never writes

- It never writes a Replay, a Certificate, or a Receipt.
- It never supplies a number the engine reports as measured.
- It never passes a file path to write or a shell command. The only query
  text it hands over is a rewrite's pairs and a keys query, which the engine
  submits read-only, under its own tags, to the one engine the operator
  named (`--twin-of`), never to the lake or a shell (ADR 033).

See [`threat-model.md`](threat-model.md) for the full list and the
reasoning.
