# Adding an index kind

A key kind touches a fixed set of places. Do them in this order, each with tests, and open one PR. WP21 (time-series keys) is the worked example once it merges: a sidecar kind (`bounds`) and a two-clock temporal kind (`temporal2`).

| Step | Where | What |
|---|---|---|
| 1. Name it in the contract | `crates/bellwether-types` (`KeySpec`) | A new variant with its columns and parameters, its `name()` spelling, and docs. Contract changes are additive only; say so in the PR under "CONTRACT CHANGE". If a lookup needs a new residual, add the `Predicate` variant here too. |
| 2. Define the bytes | `docs/format.md` | What the segment stores per key and per row group, how it is encoded (reuse the canonical encoding, ADR 016), and pinned conformance vectors. Readers that do not know a kind must ignore it; say whether yours is a sidecar beside another spec or a kind of its own. |
| 3. Write and read it | `crates/bellwether-core` | A writer and reader for the segment, the merge rule, and the lookup primitive. Tests: round trip, every-byte corruption sweep, property test that lookups equal a brute-force oracle, nulls per ADR 014. |
| 4. Build it | `crates/bellwether-build` | The builder reads the key columns it needs (one row group per worker, bounded memory, ADR 018), writes segments, records completeness (ADR 012). The planner turns a lookup into row groups and a residual; invariant 1 (no false negatives) must have its own test against decoded files. |
| 5. Price it | `crates/bellwether-replay` | A transient in-memory segment so `why` can price the kind before it exists, and the `PlanSource` rules the real planner follows, so predicted equals planned (tested). |
| 6. Surface it | `crates/bellwether` and `crates/bellwether-cli` | A spec spelling on the command line, typed results, the `--json` byte-identity test. No engine logic in the CLI. |
| 7. Measure it | `crates/bellwether-bench` | A corpus and workload where the kind matters, and a funnel in the PR body labeled with the machine and build. |
| 8. Explain it | `docs/glossary.md`, `docs/rule-catalog.md` if a lint recommends it, the crate READMEs | One paragraph a non-specialist can act on. |

Rules that apply to every kind: a number that cannot be computed exactly from statistics is labeled, never guessed; an ambiguity at a boundary is an error, never a best guess; the dependency graph in `dev/check-layering.py` does not change; and the PR is reviewed by someone who did not write it, with probe tests and mutation testing, before it merges.
