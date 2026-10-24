# Evolve: reflective pipeline optimization with an exact loss

Design document for `bellwether-evolve` (WP13, milestone M6). Governing decisions: ADR 021 (the loop), ADR 022 (what "gradient" means here), ADR 023 (joint and whole-pipeline optimization), ADR 024 (counterfactual attribution), ADR 025 (rulebook validation).

## 1. The problem, stated precisely

A data pipeline is a DAG of steps. Each step has a logical plan (a DAG of operators), reads from a physical design (which indexes and layouts exist), and runs many times with different inputs (a trigger batch of keys, a date window). Engineers tune three things: the plan (operator order, join strategy, projection), the physical design (keys, layouts, knobs), and the step boundaries (what is materialized, what is pushed upstream). Today they tune by hand, scoring with estimates and verifying by hope.

Evolve searches that space against a **recorded workload** with an **exact loss** and a **hard equivalence constraint**.

Let:

- `W = {r_1 … r_n}` be the recorded runs. Each run carries its key set, parameters, and the plan it executed.
- `a` be an artifact in the search space `A`: a physical design `D` (set of key specs, a layout, knobs), a plan `P` (operator DAG), or a pipeline `G` (DAG of steps, each with a plan), or a combination.
- `c(a, r_i) ∈ R^m` be the exact cost vector of artifact `a` on run `r_i`: bytes read, row groups read, files opened, and, when an executor is attached, measured latency. Computed by replay (ADR 005), never estimated.
- `m(a)` be the maintenance cost of the design: index bytes, build time.
- `Eq(a, a_0)` be the equivalence predicate: the pipeline output under `a` equals the output under the baseline `a_0` over the keys `W` touches, certified by reconciliation (ADR 004), or by a static identity from the rule catalog.

The optimization problem is multi-objective and constrained:

```
minimize over a ∈ A:   { c(a, r_1), …, c(a, r_n), m(a) }     (a vector of vectors)
subject to:             Eq(a, a_0)
```

No single scalar objective is used. Selection is Pareto over runs and objectives (section 6), so a candidate that wins at the median batch and loses at the p95 batch cannot eliminate the candidate that protects the tail.

## 2. Why this is tractable here and nowhere else

Two properties of Bellwether make the search sound:

1. **The loss is exact and cheap.** Replay evaluates a hypothetical design or plan by summing recorded row-group statistics under the candidate's pruning. It costs milliseconds per run per candidate and returns the same number every time. There is no estimation noise, which means finite differences are exact (section 4) and the search cannot be misled by a cost model.
2. **The constraint is checkable without a prover.** Reconciliation compares latest-version multisets and decimal totals at a step's or the pipeline's output over the touched keys. It does not care whether the rewrite was SQL, Spark, KQL, or a configuration change. This is what lets evolve certify non-local rewrites that no equivalence prover can express (section 5).

Everything else in the loop is borrowed from text-space optimization (GEPA, SkillOpt, TextGrad) and from classical physical design (AutoAdmin). What is new is the pairing.

## 3. The loop

```
population P ← { a_0 }                       baseline is always a member
archive     ← ledger                         every evaluated candidate is a record
repeat until budget:
    parent  ← select(P)                      from the Pareto frontier, crowding-distance tie-break
    trace   ← replay(parent, W)              per-run, per-operator exact costs
    attr    ← attribute(parent, trace)       counterfactual partial differences (section 4)
    child   ← propose(parent, trace, attr, rulebook)      one of the proposers (section 4)
    if not Eq(child, a_0): discard, record why; continue
    costs   ← replay(child, W)
    P       ← pareto_update(P, child, costs)
rulebook    ← distill(archive)               validated against held-out runs (section 7)
return frontier(P), receipts, rulebook
```

Every arrow in that loop writes a ledger record. The proposer's prompt and completion are stored verbatim with the model identifier. A search is therefore replayable from its own ledger: the stored completions are reused, so the result is reproducible; a fresh search is a new ledger. The trust boundary (ADR 009) is unchanged: proposers write Proposals, the engine writes Replays and Certificates, and nothing a proposer emits is ever a score.

## 4. What "gradient descent" means here (ADR 022)

The artifacts are discrete: text, DAGs, sets. There is no differentiable path from a plan to its cost. Three honest substitutes exist, and evolve uses all three with the same scorer and gate.

### 4.1 The backward pass: counterfactual attribution (ADR 024)

A gradient tells you how much each parameter contributed to the loss. For a plan, the analogue is: how much did each operator, and each design choice that fixed that operator's input, contribute to the bytes read. Because replay is exact and cheap, this is computed by counterfactual, not approximated:

- For each operator `v` in the plan, `Δ_v = c(a) − c(a ⊖ v)`, where `a ⊖ v` is `a` with `v` replaced by its best known local alternative (the identity from the rule catalog that applies there), or ablated when no alternative exists.
- For each pair of choices `(u, v)`, the interaction term `I_uv = c(a_uv) − c(a_u) − c(a_v) + c(a)`. A nonzero `I_uv` means the two choices do not compose additively, which is the signal that they must be proposed together (section 5.3).

`Δ` is the per-node partial difference. On a twenty-node plan it costs twenty replays, under a second on the synthetic ledger. It is the "backward pass" the proposers consume, and it is exact where a textual gradient from an LLM alone would be a guess.

### 4.2 Proposer one: textual gradients for structure

For discrete structure (operator order, join strategy, which key, which layout), the proposer is a language model given the parent, its per-run trace, its attribution, and the current rulebook, and asked for a critique and one mutation. The critique is the textual gradient in the sense of ProTeGi and TextGrad; the mutation is the update step. The difference from those systems is that the critique is grounded in `Δ_v`, so the model is told which operator cost what, rather than inferring it.

Mutations are expressed as plan edits with a named identity when one applies ("push semi-join on key K below window partitioned by K", identity `semijoin_before_window`), which lets the gate use a static proof for that edit and reconciliation for the rest.

### 4.3 Proposer two: exact numeric descent for knobs

A design has numeric knobs: bloom false-positive rate, the key-count gates (when to broadcast, when to push an `IN` list, when to fall back to shuffle), lookback windows, chunk and segment sizes, partition counts. These are integers or reals, and the cost as a function of them is deterministic. So:

- Finite differences are exact: `∂c/∂θ_j ≈ (c(θ + e_j) − c(θ − e_j)) / 2` has no sampling noise, only discretization.
- Coordinate descent, pattern search, and integer line search converge on these without any of the variance-reduction machinery that noisy black-box optimization needs.

This is real descent on a real objective. It is the only part of evolve that is honestly "gradient-like," and it is limited to knobs.

### 4.4 Proposer three: rule-driven enumeration

The rule catalog's detectors produce deterministic candidates (add the composite key the fanout measurement recommends; bound the lookback). These seed the population so the LLM is never asked to rediscover what measurement already says.

### 4.5 Deferred: learned surrogates

A differentiable surrogate cost model trained on replay data could propose candidates by gradient descent on the surrogate. It would be an estimator, and the thesis forbids estimators as scorers. It is admissible only as a proposer, with exact replay as the arbiter. Deferred until the three proposers above have been evaluated; recorded here so the boundary is explicit.

## 5. Interactions: joint and whole-pipeline optimization (ADR 023)

Per-call optimization is the wrong unit. Three kinds of interaction dominate real pipelines, and evolve handles each.

### 5.1 Plan-internal interactions

Moving a semi-join earlier changes the window's input; choosing a composite key changes whether a prefilter is worth having. Evolve mutates plans, not tokens, and scores the whole plan, so plan-internal interactions are captured by construction. A proposal may bundle several edits (a patch set) when the interaction matrix says they do not compose additively.

### 5.2 Workload-level physical design

One index serves many queries. The per-query optimum conflicts: a composite key that is perfect for one run is dead weight for another, and index bytes and build time are shared costs. This is the classical index-selection problem (AutoAdmin), whose weakness was always the estimate. Evolve formulates it as a **set-valued configuration**: a candidate design is a set of key specs and a layout; replay scores the set against every run; subsumption is modeled (a composite key with prefix seek subsumes its leading single-column key); maintenance cost `m(a)` is a separate objective. Search is greedy construction plus local search with exact scoring, seeded by rule-driven candidates and mutated by the LLM. Interaction terms between key specs are computed by counterfactual (4.1) and used to decide which specs to propose together.

### 5.3 Whole-pipeline equivalence and non-local rewrites

The most valuable rewrites cross step boundaries: push a filter from step five to step two, materialize an intermediate two steps share, remove a deduplication that a later step repeats. No single-query prover can certify these, because each step's local output changes. Reconciliation at the **pipeline output** can, because what matters is that the final rows are the same over the touched keys.

Evolve therefore takes the pipeline DAG with lineage (which step consumes which), allows cross-step mutations, scores the whole DAG by replay, and gates at the output with a workload-scoped certificate. Intermediate steps may legitimately differ. This is the capability that makes "optimize the pipeline" different from "optimize each query," and it is only possible because the gate is data-level.

A consequence worth stating: a certificate's coverage is the keys the workload touched. A non-local rewrite is certified for that workload, not for all inputs. The certificate says so, and the rulebook (section 7) must re-validate a rule on held-out runs before promoting it.

### 5.4 Interaction with the data itself

Fanout is a property of the data, not the query. A key that discriminates today may stop discriminating after an upstream change. Evolve records the fanout evidence each decision depended on; `verify` reports drift; a receipt whose evidence has drifted is flagged stale and the search can be re-run from its ledger.

## 6. Selection: Pareto over runs and objectives

Scalarizing cost (mean bytes over runs) hides the tail. Evolve keeps a Pareto frontier over the full matrix (runs × objectives). A candidate survives if no other candidate is at least as good on every cell and strictly better on one. Crowding distance breaks ties so the frontier stays spread. The baseline is always in the population so the frontier can never regress below it.

Two practical rules: runs are grouped by template (same plan shape, different keys) so the frontier is not dominated by run count; and `m(a)` is on the frontier so an index that halves bytes but triples storage is visible, not hidden.

## 7. The rulebook: what is learned, and how it is validated (ADR 025)

The frontier is the answer for this workload. The rulebook is what transfers. After search, winning mutations are clustered by identity and pattern; for each cluster the LLM writes a rule entry: detector (what pattern on evidence or plan), action, invariant, and the evidence hashes and measured gains that justify it. A rule is **not accepted on the LLM's word**. It is promoted only if applying it to held-out runs (runs excluded from the search) reproduces a gain under exact replay and passes the gate. Rules carry confidence from that validation. This is SkillOpt's validation gate applied to rules instead of skills, and it is what stops the rulebook from accumulating folklore.

The rulebook is a text artifact with hash links, read by agents on the next `fix`, and it is the learned analogue of a hand-written optimization catalog. It is also the paper's transfer result: a rulebook learned on one synthetic ledger applied to held-out query templates.

## 8. Determinism and the ledger

- A search is pinned to: manifest sequence, workload hash, rule catalog version, proposer identifiers, and a seed.
- Every candidate, score, certificate, attribution, interaction matrix, frontier snapshot, LLM prompt and completion, and rule is a ledger record.
- Replaying a search from its ledger reuses stored completions and reproduces the frontier byte for byte (invariant 3 extended).
- Budgets are explicit: replays, LLM calls, wall clock. Exhaustion is a recorded terminal state, not an error.

## 9. Threat model extension

The proposer may be wrong, adversarial, or compromised. It can: waste budget, propose non-equivalent artifacts (discarded at the gate), and write misleading rule text (rejected by held-out validation). It cannot: score, pass the gate, apply, or promote a rule. A surrogate, if ever added, is a proposer and inherits the same limits. Budgets bound the damage of a proposer that only wastes.

## 10. Contract additions (v3, after M3)

Traits: `Scorer` (replay; or an external engine's measured cost) and `Verifier` (reconcile; result-hash over sampled parameters; static identity). Records: `Candidate` (artifact plus parent hash and diff), `Attribution` (per-node `Δ`), `Interaction` (matrix), `Frontier` (snapshot), `ProposerCall` (prompt, completion, model id), `Rule`, `Rulebook`. All content-addressed; all carry provenance.

## 11. Evaluation design (fixed before results, ADR 019)

Dataset: the synthetic versioned ledger with planted anti-patterns and a pipeline of several dependent steps. Workload: recorded runs at p50, p95, max key-set sizes, split into search and held-out sets.

Baselines: no index; Parquet statistics; DuckDB; Apache DataFusion; an LLM-only rewriter without replay (GenRewrite shape); an estimate-based advisor (a DuckDB or DataFusion cost estimate standing in for a what-if API).

Metrics:

- Cost reduction per run at p50, p95, max; and `m(a)`.
- Recovery rate of planted anti-patterns, blind.
- Realized versus predicted cost: zero error for evolve by construction; measured error for the estimate-based baseline.
- Search efficiency: gain per replay and per LLM call; ablations for Pareto versus mean selection, with versus without attribution, with versus without the gate.
- Non-local rewrites found that no local identity could certify, and whether the output certificate held.
- Rulebook transfer: gain on held-out templates from rules alone.
- Tail protection: p95 cost under Pareto versus mean selection.

Figures, all rendered from ledger records by `bellwether report`, never recomputed for display, and exported as CSV by `bellwether report --paper` so the paper's pgfplots templates (`paper/data/README.md`) switch from placeholders to real plots automatically:

1. Pruning funnel per run and strategy (log-scale grouped bars).
2. Latency versus corpus size (log-log lines with p95 bands).
3. Realized versus predicted bytes (log-log scatter with the diagonal; exact replay lies on it by construction).
4. Convergence per generation with p50, p95, max run bands.
5. Pareto frontier evolution in two run-cost axes across generations.
6. **Loss landscape with the descent path**: exact replay cost over two numeric knobs evaluated on a grid, drawn as a three-quarter-view 3D surface and as a contour map, with the finite-difference descent path overlaid. This figure exists only because the loss is exact and cheap enough to evaluate everywhere, which no estimate-based advisor can offer.
7. Counterfactual attribution heatmap (operators × runs) and the pairwise interaction matrix.
8. Rulebook transfer: held-out gain per rule with confidence, and cumulative gain.

Three schematics with no measured quantities accompany the method: the Pareto-over-runs selection, the backward pass on a plan DAG, and whole-pipeline equivalence with a pushed filter.

## 12. Latency budgets

On the synthetic ledger at the medium size: one replay of one run under one candidate under 5 ms; attribution of a 20-node plan under 1 s; one generation of 16 candidates under 1 min excluding proposer latency; a full search of 50 generations under 1 h. Asserted by the bench harness.

## 13. Open questions, recorded rather than hidden

- Certificate coverage for non-local rewrites is workload-scoped. How to communicate that to a user who then runs an untouched key.
- Rule distillation may overfit to the synthetic ledger's planted patterns. The held-out split mitigates; a second generator with different patterns is the real test.
- Attribution by ablation is one replay per node. Plans with hundreds of nodes need sampling, which reintroduces estimation; the design caps attribution at the top-k nodes by cost rather than sampling.

## 14. Operators priced exactly (ADR 026)

The loss covers the relational subset a lake pipeline actually uses, not lookups alone. Each row says what the price needs and how exact it is.

| Operator | Needs | Exactness |
|---|---|---|
| Projection of columns | per-column chunk bytes from footers (`ColumnStats.bytes`) | exact |
| Equality, `IN` | exact or bloom key | exact (bloom: bounded by its false-positive rate) |
| Range | min/max statistics; exact under a range key | bounded; exact with a range key |
| `latest`, `asof` | temporal key | exact |
| Semi-join of a key set against a dataset | exact key on the dataset | exact |
| Inner or left join of two datasets | exact keys on both join columns; counts for output cardinality | exact row groups; exact cardinality with counts, else bounded |
| Window partition prefilter | exact key on the partition columns | exact |
| Aggregation input | projection and the filters above | exact |
| Shuffle bytes | rows per key (counts) and average row width per column | bounded |
| CPU and wall time | an attached executor | measured |

Counts are per-key, per-row-group row counts stored beside the bitmap when a key spec asks for them (storage version 2). They cost index space and buy exact join cardinalities, exact layout pricing, and exact shuffle bounds. The optimizer's backward pass (§4.1) is unchanged: one exact replay per counterfactual, whatever the operator.

