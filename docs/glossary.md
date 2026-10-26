# Glossary

Every term the README and docs use, in one place, in plain language. If a term is not here, it should not be in a user-facing document.

| Term | Meaning |
|---|---|
| **Row group** | A horizontal slice of a Parquet file, typically tens of thousands of rows, with its own statistics. The unit Bellwether prunes at. For CSV and JSON lines, a fixed byte-range chunk plays the same role. |
| **Cardinality** | The number of distinct values in a column. High for an id, low for a status. |
| **Fanout** | How many files or row groups one key value points at. Low fanout is what makes a key worth indexing. A batch timestamp has high fanout even when its cardinality looks reasonable. |
| **Selectivity** | The inverse of fanout: how much of the data one lookup excludes. |
| **Key spec** | What is indexed: a list of columns (one or more, so composite keys are the normal case) and a kind: exact, bloom, range, or temporal. |
| **Composite key** | A key over several columns, encoded so a lookup on the leading columns alone still seeks. |
| **Temporal key** | A key that also records, per value, the newest timestamp and where it lives, so `latest` and `asof` are lookups rather than sorts. |
| **Segment** | An immutable index file: sorted keys, bloom filters per block, and a bitmap of row groups per key. Named by the hash of its bytes. |
| **Manifest** | The small file that lists which segments are current, assigns row-group ids, records file fingerprints, and carries a sequence number that goes up by one per commit. |
| **Conditional PUT** | Writing a file only if nobody else wrote it first. How the manifest is committed without locks. |
| **Fingerprint** | How a file is recognized: path, size, etag or modification time, and a content hash. A changed fingerprint means the old index entries for that file are retired. |
| **Scan plan** | The output of a lookup: which files and row groups to read, with exact bytes, and a residual predicate to apply if you want row-exact results. |
| **Residual predicate** | The filter a consumer applies after reading the planned row groups, because pruning selects whole row groups and can include extra rows. |
| **Replay** | Computing exactly what a recorded workload would have read under a given index or plan, from statistics alone, without reading data. The cost model, and it never estimates. |
| **Workload** | The recorded key sets of real runs, used by replay and by evolve. |
| **Reconcile** | Comparing two datasets by key, newest version, row hash, and exact decimal totals. The verifier. |
| **Certificate** | The record a reconciliation produces: what was compared, how much of the key space, and whether it passed. |
| **Evidence** | What `why` and `profile` write: measured fanout, lints, and the inputs they came from. |
| **Proposal** | What an agent (or a person, or a rule) suggests: a key, a layout, or a rewrite. Has no score field, on purpose. |
| **Receipt** | The record that an apply happened, linking the evidence, proposal, replay, and passing certificate. The only record a reviewer or a gate needs to read. |
| **Ledger** | The append-only, content-addressed store of all the records above. |
| **Trust boundary** | The line between what an agent may do (observe, propose) and what only the engine may do (score, verify, apply). |
| **Evolve** | The optimizer: a population search over plans, keys, layouts, and pipelines, scored by replay, gated by reconcile, selected by Pareto over runs. |
| **Counterfactual attribution** | For each operator in a plan, the exact cost difference from replacing it with its best alternative. The optimizer's backward pass. |
| **Pareto frontier** | The set of candidates none of which is beaten on every run and objective at once. Keeps the candidate that protects the worst run. |
| **Rulebook** | Rules distilled from winning mutations, each promoted only when held-out replay reproduces its gain. The learned, transferable artifact. |
| **Lint** | A named finding from the rule catalog, such as `batch_stamp_key`, with a plain-language message and a confidence. |
| **Sketch** | Serves `count(distinct col)` and the question "is this column a key" without reading the data. A `sketch:col[,col][,a+b]` sidecar keeps, per column (or `+`-joined tuple) per file, a fixed-size HyperLogLog summary of its distinct values, and optionally a MinHash signature for comparing two columns' value sets. `bellwether sketch cardinality` then reports the approximate distinct count with its error bound, rows per value, and files that repeat a value; `sketch overlap` reports how much two columns' values overlap; `why --sketches-only` diagnoses a folder (including the low-cardinality-key lint `R-sketch-1`) with no data file read. Every number from a sketch is labelled approximate. **The planner never reads a sketch** (`schema.sidecars[]` holds structures the planner never reads): an estimate cannot decide which row groups to skip without risking a missed row, so a sketch cannot be proposed, priced, or receipted, and it never changes a plan. |
