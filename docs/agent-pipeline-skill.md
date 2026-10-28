# Bellwether Pipeline Repo Agent Skill

Operational playbook for a coding agent editing a SQL, PySpark, DataFusion, DuckDB, or dbt pipeline repository with Bellwether MCP attached (`examples/mcp.json`: `bellwether-mcp . --workspace`).

In `--workspace` mode, pass `dataset: "<relative-folder>"` (for example `"data/orders"` or `"orders"`) on any tool call to target that dataset folder inside the workspace; omit `dataset` when the server is bound to a single folder.

---

## Workflow A: Physical Design & Reader Pinning (No Output Schema Change)

Use this workflow when a pipeline step reads far more bytes or row groups than it returns and the output schema is unchanged.

1. **Translate or profile the slow query.**
   - Single lookup or SQL query: call `observe.why` (with `dataset`) to get `evidence.hash`, or `observe.plan_from_sql` (`sql`, optional `main_table`, `as_of`, `dataset`) to translate a read-only `SELECT` into a typed `ConsumerPlan` DAG, then `score.why` (`keys` or inline `key_values` with `key_columns`, optional `plan`, `at`, `dataset`, `wait: true`) to get the funnel, the culprit, the recommended key, and `workload.hash`.
   - Multi-query workload: call `score.advise` (`workloads` or `keys`, optional `max_keys`, `max_index_bytes`, `delta`, `gates`, `dataset`, `wait: true`) to get the Pareto frontier and knee-point recommendation.
2. **Propose, verify, and apply the index.**
   - Call `propose.key` (`spec`, `evidence: [<evidence.hash>]`, `by: "<agent>"`, optional `sidecars`, `dataset`), or take the engine proposal hash from `score.why` / `score.advise`.
   - Call `score.verify` (`proposal`, `workload`, `dataset`, `wait: true`) to score by exact replay and reconcile the planned row groups against a full scan (`certificate.hash`).
   - Call `apply.receipt` (`certificate`, `dataset`, `wait: true`) to build the index in one manifest commit and record the `Receipt` (`outcome.receipt.hash`).
3. **Emit the pinned engine scan and wire it into the pipeline.**
   - Call `observe.locate` or `observe.asof` with `keys` or `key_values` and `emit: "duckdb" | "datafusion" | "spark"` (plus optional `ordered`, `residual`, `relocate`, `dataset`).
   - Wire `emitted.text` into the pipeline step:
     - **DuckDB:** execute the emitted `SELECT ... FROM read_parquet([...]) WHERE ...` statement.
     - **DataFusion:** pass the emitted `ScanConfig` JSON to `bellwether_adapters::datafusion::scan_parquet`.
     - **PySpark:** pass the emitted manifest JSON to `shims/spark/bellwether_plan.py` (`read_planned(spark, manifest)`).

---

## Workflow B: Query / Pipeline Step Rewrite (With CI Receipt Gate)

Use this workflow when rewriting a SQL query or pipeline step (for example pushing a key semi-join below an aggregate or deduplicating before a join) in a repository gated by `bellwether ci` (`.bellwether/ci.toml`).

1. **Translate before/after plans and materialize sample outputs.**
   - Call `observe.why` (`dataset`) to record `evidence.hash` (or `score.why` with `plan` to record `workload.hash` and a `PlanShape` culprit).
   - Call `observe.plan_from_sql` on the original SQL (`before_plan`) and the rewritten SQL (`after_plan`).
   - Run the before and after pipeline steps on a staging or sample window to `before_out/` and `after_out/` (or, for a warehouse twin started with `--twin-of` and `--scratch`, prepare `shape.pairs: [{ before, after }]`).
2. **Propose the rewrite.**
   - Call `propose.rewrite` citing `evidence: [<evidence.hash>]` and `by: "<agent>"`, with `shape: { before, after, invariant, before_plan, after_plan, keys: "<workload.hash>" }` (or `shape: { before, after, invariant, pairs, key }` for an engine rewrite).
3. **Verify and attest.**
   - Call `score.verify` (`proposal`, `workload`, `rewrite: { before: "<before_out>", after: "<after_out>", key: ["<key_col>"], totals: ["<dec_col>"], latest_by: ... }`, `dataset`, `wait: true`) to price both plans and reconcile the outputs (`certificate.hash`).
   - Call `apply.receipt` (`certificate`, `dataset`, `wait: true`) to obtain the attest-only `Receipt` hash (`outcome.receipt.hash`).
4. **Record the receipt for CI.**
   - Write the 64-character hex receipt hash to `.bellwether/receipts/<step>` or add `Bellwether-Receipt(<step>): <hash>` to the pull request description so `bellwether ci` (`action/action.yml`) passes.

---

## Workflow C: Self-Healing Stale Indexes

When a tool refuses with `stale_index` or `stale_join` because files landed, changed, or were compacted since the index was built:

1. Call `apply.refresh` with `dry_run: true` (and `dataset` when in workspace mode) to inspect `untracked`, `changed`, `missing`, and stale join keys without writing.
2. Call `apply.refresh` with `wait: true` (and `dataset`) to rebuild all declared key specs, sidecars, and join keys in one manifest commit, then retry the original tool call.
