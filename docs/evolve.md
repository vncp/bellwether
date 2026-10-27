# Evolve: reflective pipeline optimization with an exact loss

Design document for `bellwether-evolve` (WP13a `advise`, WP13b `evolve`, WP13c rulebook v2; milestone M6). Governing decisions: ADR 021 (the loop), ADR 022 (what "gradient" means here), ADR 023 (joint and whole-pipeline optimization), ADR 024 (counterfactual attribution), ADR 025 (rulebook validation), ADR 031 (scoped proposers and a frozen outer loop), ADR 032 (staged delivery with a measured trigger).

**Scope as of 2026-10-27 (ADR 032 amendment; WP13a / M6 shipped).** Reduced stage 1 (`crates/bellwether-evolve`, `Engine::advise`, `bellwether advise`, MCP `score.advise`) is built: workload-level key-set selection with subsumption, exact knob descent (bloom FPR grid and key-count gates), Pareto frontier with NSGA-II crowding distance and knee-point selection, preserve-and-extend anchor admission with $\delta$, and receipts (`--apply`, §3.1). Layout and materialization search, edge and plan-node attribution, stage 2 (`evolve --llm`), and stage 3 (rulebook v2) are deferred until a recorded multi-step pipeline workload exists and reduced `advise` leaves a structural anti-pattern on it. The rest of this document is the design for that case.

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

### 2.1 What transfers from harness and prompt optimization, and what does not

Most harness-evolution results are compensations for a noisy or absent verifier. Bellwether has neither problem, so the transfer is selective. Apply the left column; do not import the right.

| Transfers (and why it still applies with an exact loss) | Does not transfer (and why) |
| --- | --- |
| **Scoped proposers.** Harness²'s largest ablation was splitting the editor into an invariant scope (how the system works) and a domain scope (what this workload needs): −2.9 to −3.7 points without it. The split is about what evidence each proposer reads, not about scoring | **On/off canaries, free-signal counters, redirect mining.** Those exist to verify without a verifier. Replay and reconcile verify directly |
| **Composition over selection.** Merging scoped candidates by evidence beat picking one: +1.6 to +1.9. Here the composer can be code, driven by the interaction matrix (ADR 024) | **Judge hardening, rubric design, pairwise dueling.** No judge exists in this loop |
| **Preserve-and-extend admission.** DarwinX: admit only if net gain > 0 *and* summed regressions ≤ δ; HCL: a small nonzero regression budget beat both zero and unlimited | **Minibatch screening and racing.** Replay costs milliseconds per run; racing only matters when `Scorer` is a measured executor |
| **Temporal holdout.** 54% of reported harness gains were overfitting under random splits; holding out the newest runs models deployment | **Skill packages, unit retrieval, token budgets.** No context window is being managed |
| **Libraries rot.** Every unbounded library peaked then declined; caps forcing merges, counters, and retirement are the fix. Reused components were rarely copied verbatim (2–10%), so rules must be re-fit, not re-applied | **Per-task test-time recursion.** The workload is the task; there is no per-instance rerun to buy |
| **Generalization lint.** HarnessCompass's gate (no task ids, no string-match branches, every rule carries an applicability condition) is the right shape for rule text | **Evolving the verifier.** Verifiers selected by agent score collapsed to always-pass while scores rose. Reconciliation stays fixed; only catalog *detectors* may be proposed, and they are admitted by held-out replay |
| **Frozen outer loop with an editable improvement record.** Hyperagents' finding that a self-editing meta-level outruns audit; Harness²'s that an experience library helps proposals (−3.1 without it). Safe here because gain per call is measured exactly | **Learned surrogates as scorers.** Confirmed by VERSE: self-evolution without execution-based verification scored below a blank baseline. Surrogates remain proposer-only (§4.5) |
| **Strong-to-weak split.** AI4AI: a strong builder, a cheap target. Here: cheap proposers, strong composer only when interactions are non-additive | **Statistical margins, noise floors.** Replay is deterministic; the floor is zero |

Where each row lands: scoped proposers, the experience record, and the frozen outer loop in ADR 031; composition, preserve-and-extend admission, and anchors in ADR 021; the generalization lint in ADR 022; the temporal holdout, counters, caps, and re-fit in ADR 025; the strong-to-weak split in WP13b.

## 3. The loop

```
population P ← { a_0 }                       baseline is always a member
archive     ← ledger                         every evaluated candidate is a record
repeat until budget:
    parent  ← select(P)                      from the Pareto frontier, crowding-distance tie-break
    trace   ← replay(parent, W)              per-run, per-operator exact costs
    attr    ← attribute(parent, trace)       counterfactual partial differences (section 4)
    cands   ← propose(parent, trace, attr, rulebook)      the proposers of the current stage (section 4, §3.1)
    for child in compose(cands, attr):       bundles pairs with |I_uv| above a threshold (ADR 021)
        if not Eq(child, a_0): discard, record why; continue
        costs ← replay(child, W ∪ anchors)
        if not admit(child, parent, costs, δ): discard, record why; continue    preserve-and-extend
        P     ← pareto_update(P, child, costs)
rulebook    ← distill(archive)               validated against held-out runs (section 7)
return frontier(P), receipts, rulebook
```

Every arrow in that loop writes a ledger record. The proposer's prompt and completion are stored verbatim with the model identifier. A search is therefore replayable from its own ledger: the stored completions are reused, so the result is reproducible; a fresh search is a new ledger. The trust boundary (ADR 009) is unchanged: proposers write Proposals, the engine writes Replays and Certificates, and nothing a proposer emits is ever a score.

Admission (ADR 021, amended 2026-10-25) is **preserve-and-extend**: a child enters the population only if its net gain over the parent across runs is positive *and* the sum of its per-run regressions is within a declared budget δ, with δ small and nonzero. The held-out **anchor set** (the p95 and max key-set runs per template) is replayed on every admission and may not regress beyond δ. With an exact scorer δ applies per run; with a measured one it is stated in the scorer's own units (§15).

### 3.1 Stages, triggers, and the kill criterion (ADR 032)

Keep evolve. It is the one place in this field where the optimizer has an exact loss (replay, ADR 005) and a hard constraint (reconciliation, ADR 004), and every published harness or prompt optimizer is a workaround for lacking those. But ship it in three stages, each useful alone, with a measured trigger between them, because the generative gain is front-loaded in the parts that need no language model.

| Stage | What ships | Model calls | Proceed to the next stage only if |
| --- | --- | --- | --- |
| 1 · `advise` | Rule-driven enumeration (§4.4) + exact numeric descent on knobs (§4.3) + layout and materialization search, all scored by replay and gated by reconcile; Pareto over runs × objectives with anchors | None | On the synthetic ledger and one real workload, the frontier still leaves planted anti-patterns unrecovered that only a structural rewrite can fix |
| 2 · `evolve` | One scoped structural proposer (invariant and workload scopes) fed by attribution, a deterministic composer, and the improvement-experience record | Bounded, cheap model by default | Stage 2 recovers rewrites stage 1 cannot, at a cost per accepted mutation below a stated budget |
| 3 · rulebook v2 | Rules as predicated templates with counters, temporal holdout, caps, staleness that must be re-earned | Distillation only | A rule learned on one workload reproduces a gain on a held-out template under replay |

**Kill criterion for stages 2 and 3.** If stage 1 captures the frontier on the real workloads you have (frontier within 5% of stage 2's on bytes read at p50 and p95), stop. An exact what-if advisor with receipts is already a product; the language-model search is the part that must prove it adds rewrites enumeration cannot reach.

**Reduced stage 1 (2026-10-27, ADR 032 amendment).** The stage-1 row above now ships without layout and materialization search: `advise` chooses a set of key specs and the numeric knobs replay prices, against every recorded run, with Pareto, anchors, and receipts. It starts after WP22b's lookup fix (plan time under full-scan time), because the loss is bytes read and on 2026-10-26 fewer bytes did not yet mean less time. Stages 2 and 3 are not scheduled; the trigger and kill criterion above apply when they are reopened.

**The stage-2 budget.** Cost per accepted mutation is replays plus model calls charged to one budget (§11, item 2). Its bound is the owner's decision and is deliberately unset here; it must be written into this section and recorded as a ledger row before WP13b's first search, as ADR 019 requires of every threshold an evaluation is judged by. WP13b's done-when cannot be met until it is.

Each trigger is evaluated from ledger records and recorded as a ledger row, so the decision to open the next stage is itself replayable. In stage 1, `propose` runs only the deterministic proposers (§4.3, §4.4) plus layout and materialization search; the composer and admission rule above apply in every stage.

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

From stage 2 (ADR 031, WP13b) this proposer is two prompts with different evidence slices: an *invariant* proposer (identities, operator order, step fusion and fission, materialization) and a *workload* proposer (this workload's keys, layouts, cadence, knob starting points). Both read the attribution record and matching `Experience` records; neither reads a score. Every structural proposal passes the generalization lint (ADR 022) before any replay.

### 4.3 Proposer two: exact numeric descent for knobs

A design has numeric knobs: bloom false-positive rate, the key-count gates (when to broadcast, when to push an `IN` list, when to fall back to shuffle), lookback windows, chunk and segment sizes, partition counts. These are integers or reals, and the cost as a function of them is deterministic. So:

- Finite differences are exact: `∂c/∂θ_j ≈ (c(θ + e_j) − c(θ − e_j)) / 2` has no sampling noise, only discretization.
- Coordinate descent, pattern search, and integer line search converge on these without any of the variance-reduction machinery that noisy black-box optimization needs.

This is real descent on a real objective. It is the only part of evolve that is honestly "gradient-like," and it is limited to knobs.

### 4.4 Proposer three: rule-driven enumeration

The rule catalog's detectors produce deterministic candidates (add the composite key the fanout measurement recommends; bound the lookback). These seed the population so the LLM is never asked to rediscover what measurement already says.

### 4.5 Deferred: learned surrogates

A differentiable surrogate cost model trained on replay data could propose candidates by gradient descent on the surrogate. It would be an estimator, and the thesis forbids estimators as scorers. It is admissible only as a proposer, with exact replay as the arbiter. Deferred until the three proposers above have been evaluated; recorded here so the boundary is explicit. The boundary is supported by VERSE: self-evolution without execution-based verification scored below an unmodified baseline (§2.1).

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

Rulebook v2 (ADR 025, amended 2026-10-25; WP13c): a rule is a parameterized template with an applicability predicate over evidence shape and parameters re-fit on each new workload, never a workload constant; the held-out set is temporal (the newest runs by recorded time); `helpful` and `harmful` counters from later searches retire rules that stop paying; per-detector-family buckets are capped, so a full bucket admits only a merge or revision; a stale rule re-earns promotion on the new runs or is retired.

The rulebook is a text artifact with hash links, read by agents on the next `fix`, and it is the learned analogue of a hand-written optimization catalog. It is also the paper's transfer result: a rulebook learned on one synthetic ledger applied to held-out query templates.

## 8. Determinism and the ledger

- A search is pinned to: manifest sequence, workload hash, rule catalog version, proposer identifiers, and a seed.
- Every candidate, score, certificate, attribution, interaction matrix, frontier snapshot, LLM prompt and completion, and rule is a ledger record.
- Replaying a search from its ledger reuses stored completions and reproduces the frontier byte for byte (invariant 3 extended).
- Budgets are explicit: replays, LLM calls, wall clock. Exhaustion is a recorded terminal state, not an error.

## 9. Threat model extension

The proposer may be wrong, adversarial, or compromised. It can: waste budget, propose non-equivalent artifacts (discarded at the gate), and write misleading rule text (rejected by held-out validation). It cannot: score, pass the gate, apply, promote a rule, or edit the outer loop (scorer, verifier, gate, Pareto rule, anchor set, and splits are fixed inputs; a proposal touching them is rejected by lint and recorded as an attempt, ADR 031). A surrogate, if ever added, is a proposer and inherits the same limits. Budgets bound the damage of a proposer that only wastes.

## 10. Contract additions (v3, after M3)

Traits: `Scorer` (replay; or an external engine's measured cost) and `Verifier` (reconcile; result-hash over sampled parameters; static identity). Records: `Candidate` (artifact plus parent hash and diff), `Attribution` (per-node `Δ`), `Interaction` (matrix), `Frontier` (snapshot), `ProposerCall` (prompt, completion, model id), `Rule`, `Rulebook`. All content-addressed; all carry provenance.

Added 2026-10-25, first in WP13a: `Layout` and `Cadence` artifacts (ADR 023); `Attribution` over lineage edges as well as nodes (ADR 024); `Anchor` (the held-out anchor set, ADR 021). In WP13b: `Experience` (ADR 031). In WP13c: `Rule` gains its applicability predicate, re-fit parameters, and `helpful`/`harmful` counters (ADR 025).

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

Additions (2026-10-25, before any evolve result exists), in this order of importance:

1. **Temporal split.** Search on older recorded runs, hold out the newest by recorded time; report both the random-split and temporal-split gains so the overfitting gap is visible.
2. **Matched-budget baseline.** Random enumeration with the same number of replays, and `advise` alone, as arms beside `evolve`; charge proposer model calls and replays to one budget and plot gain against it.
3. **Anchor regressions.** Report the count and magnitude of anchor runs that regressed for every admitted child, not only the frontier.
4. **Cost per accepted mutation,** by proposer scope and by mutation class, from the `Experience` records.
5. **Transfer with re-fit versus verbatim.** For each promoted rule, the held-out gain when parameters are re-fit on the new template versus applied as learned.
6. **Tail across the pipeline.** p95 end-to-end bytes and, with an executor attached, p95 wall time, not per-step tails.
7. **Stage-1 sufficiency.** The share of planted anti-patterns recovered by `advise` alone, which is the number that decides whether 13b ships.

Unchanged and still right: recovery of planted anti-patterns blind, realized-versus-predicted on the diagonal by construction, ablations for Pareto versus mean selection, with versus without attribution, with versus without the gate.

Figures, all rendered from ledger records by `bellwether report`, never recomputed for display, and exported as CSV by `bellwether report --paper` so the paper's pgfplots templates (`paper/data/README.md`) switch from placeholders to real plots automatically. A figure renders when its records exist; no figure is a delivery gate for a work package:

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

The loss covers the relational subset a lake pipeline actually uses, not lookups alone. Relational replay (WP15) prices a consumer's plan, a typed DAG (`ConsumerPlan`), node by node from the index and footer statistics. Each row says what the price is and how exact it is; the README's table ("Pricing a whole query") is the same one.

| Operator | Bytes | Output rows |
|---|---|---|
| `scan`, `project` | projected columns' compressed chunk bytes over the planned row groups (a text chunk whole): exact | footer row counts: exact |
| `filter` with `in` or a semi-join, `semi_join`, `window` with keys | the row groups the serving exact, bloom, or temporal key plans (a bloom's false positives are in the plan): exact | exact from a counted key on exactly those columns over a scan's rows; else bounded `[(key, row group) pairs present, rows of the planned row groups]` |
| `filter` with `between` | the row groups a range key (else min/max) keeps: exact | bounded `[0, rows of those row groups]` |
| `latest`, `as_of` | the winners' row groups through a temporal key: exact | bounded `[keys with a version, counted rows of the winners' row groups]` |
| `join` (inner, left), a semi-join keyed by another node | per side the row groups holding a common key, from a merge of both indexes' sorted key blocks: exact | Σ count_L × count_R (left: plus unmatched left rows): exact with counts on both sides over scans' rows selected only by whole keys on the join columns, else bounded; a side with no exact or temporal key on its join columns is refused |
| `aggregate` | its input's: exact | groups: exact from an exact or temporal key on the grouping columns over a scan's rows, else bounded `[min(1, low), high]` |
| `exchange` | compressed-equivalent bytes moved (rows × average compressed row width): bounded with respect to that quantity, not network or memory bytes | unchanged |
| any node over a SQL table | measured, naming the model | measured |
| CPU and wall time | not priced: an attached executor measures them | |

Counts are per-key, per-row-group row counts stored beside the bitmap when a key spec asks for them (storage version 2). They cost index space and buy exact join cardinalities and exact filter rows. A shared node (a diamond, a self-join) is evaluated once per run and its scans read once with the union of their consumers' row groups; each consumer prices its rows from its own selection. Layout pricing and plan-node counterfactuals were cut from WP15 (ADR 032, amended 2026-10-27); the backward pass (§4.1) over key specs is unchanged.

## 15. Generic use through Scorer and Verifier

Nothing above assumes Parquet. Evolve stays generic as long as every artifact is scored through `Scorer` and gated through `Verifier` (contract v3) and nothing in the loop reads a file format directly. The three target shapes map as follows; each is a trait implementation plus a step parser, not a new loop.

| Target | Artifact DAG | Scorer | Verifier | Layout and cadence mean |
| --- | --- | --- | --- | --- |
| Parquet lake (today) | Steps over sources; plans over row groups | Exact replay from footers (ADR 005, ADR 026) | Reconciliation at pipeline output (ADR 004) | Sort and partition keys per intermediate; incremental via `latest`/`asof` |
| SQL script file | Statements parsed into steps with lineage from reads and writes; a SQL table is a planned source (ADR 027) | Measured cost from the engine for planned sources, exact replay where the source is a lake | Reconcile on output tables; result-hash over sampled parameters for statements without a key | Clustering or sort keys on intermediate tables; batch window per statement |
| Time series | Steps over append-only history files with temporal keys | Exact replay; `asof` lookups priced exactly | Reconcile latest-version multisets and decimal totals by key | Partition by time, sort by key within; cadence is the dominant lever (window, recompute policy) |

**Rules the generic version must keep**

- A measured `Scorer` is noisy where replay is exact. When the scorer is measured, racing (evaluate candidates on a growing sample of runs) is allowed, and the regression budget δ is stated in the scorer's own units. When the scorer is exact, no racing and δ applies per run.
- The verifier never evolves. Catalog detectors may be proposed; reconciliation and result-hash are fixed.
- Rule templates are keyed by evidence shape, never by target type, so a rule learned on a lake ("push a selective semi-join below a window partitioned by the same key") is eligible on a SQL script when its predicate matches.
- Certificates stay workload-scoped and say so; a non-local rewrite certified on one workload is a hypothesis on another.

**On the broader thesis.** The same loop shape, evidence over estimates and a gate the proposer cannot pass by itself, moved from a data-lake optimizer to an agent-harness optimizer and back. That transfer worked because the invariant parts were kept invariant: exact scoring where possible, hard gates, provenance on every claim, and a proposer that proposes and nothing more. Keep those four and evolve generalizes; soften any one and it becomes another estimate-based advisor.

