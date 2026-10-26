# Changelog

All notable changes are documented here. The format follows Keep a Changelog; versions follow Semantic Versioning. Until 1.0, the on-disk format version in `bellwether-types::format` is the compatibility contract, not the crate version.

## [Unreleased]

### Added

- `bellwether-mcp` (WP12): the MCP server over stdio (`bellwether-mcp`, and `bellwether mcp` with the same flags from one definition), tools in observe, score, propose, apply, and jobs tiers over the facade, each tool's text the CLI's `--json` line (ADR 010). Refusals carry a stable code, facts, and a `next` line in MCP field names; the facade's typed errors (`RecordNotFound`, `WrongKind`, `NoWorkload`, `NotIndexed`, `InvalidArgument`) name the argument to fix. Per-session budgets admit score and apply calls one at a time by reservation; a job id always names one result; a key already complete is refused before it is scored, so the apply dry run and the apply agree and the refusal writes nothing.
- `bellwether` (the facade, WP9): one `Engine` whose operations (`why`, `index`, `locate`, `latest`, `asof`, `propose`, `reconcile`, `fix`, `receipt`, `report`) return typed results that are the CLI's `--json` output and what the MCP server returns (ADR 010).
- `why` profiles the data (WP5): fanout, lints, and a `profile` Evidence, recorded where the index location is writable and returned unrecorded otherwise; the profile's serving candidates join the floor, every engine Proposal cites the Evidence, and the culprit names a batch-stamp or low-selectivity lint first, with its fanout.
- `bellwether-cli` (WP9): the `bellwether` binary over the facade, with human and `--json` output, the exit-code contract, and `bench` over the benchmark crate.
- Repository skeleton: workspace, CI, license, contributing guide.
- `bellwether-types`: shared records (`Evidence`, `Proposal`, `Replay`, `Certificate`, `Receipt`), `KeySpec`, `ScanPlan`, `Source` and `Store` traits, format version constants.
- Architecture document, invariants, threat model, on-disk format, and ADRs 001 through 019.
- README in three screens: a sixty-second demo of `why` and `fix` with
  illustrative output, a ten-minute tutorial outline over the synthetic
  ledger, and links to the deeper docs (WP11).
- `docs/rule-catalog.md`: seven lints in a fixed template (detector,
  estimator, guidance template, verification method, confidence rubric).
- `docs/agent-loop.md`: the four stages with a worked example and the JSON
  shape of every ledger record, matching `bellwether-types`.
- `docs/benchmark.md`: benchmark methodology per ADR 019, written before any
  result exists.
- Issue forms for bugs and features, and a pull request template asking for
  work package, invariants and ADRs touched, testing, and contract changes.
- `docs/evolve.md`: design of the reflective optimizer (milestone M6): exact
  replay loss, reconciliation gate, counterfactual attribution as the backward
  pass, exact finite-difference descent for knobs, set-valued physical design,
  whole-pipeline equivalence, Pareto selection over runs, validated rulebook.
- ADRs 020 (manifest sequence number), 021 (reflective optimization), 022
  (what gradient means here), 023 (joint and whole-pipeline optimization),
  024 (counterfactual attribution), 025 (rulebook validation); amendments to
  ADR 008 (bloom hash input) and ADR 017 (scope of unknown-field
  preservation).
- Evolve is planned in three stages (WP13 split into 13a `advise`, 13b `evolve`, 13c rulebook v2), each opened by a measured trigger with a kill criterion in `docs/evolve.md` §3: ADR 031 (scoped proposers and a frozen outer loop) and ADR 032 (staged delivery with a measured trigger); amendments to ADR 021 (stages, a deterministic composer, preserve-and-extend admission, an anchor set), ADR 022 (generalization lint), ADR 023 (layout and cadence as artifacts), ADR 024 (attribution over lineage edges, bisect on a failed output certificate), and ADR 025 (parameterized templates, temporal holdout, `helpful`/`harmful` counters, bucket caps); `docs/evolve.md` gains what transfers from harness optimization (§2.1), seven evaluation additions (§11), and generic use through `Scorer` and `Verifier` (§15). Docs only; no evolve code yet.
- `docs/format.md`: dataset-schema scale and unit for decimals and
  timestamps, statistics semantics, per-kind segment layouts, `bloom_hash`
  header field, pinned encoding test vectors.
- README rewritten: tagline, loop diagram, highlights, comparison table,
  what the project deliberately is not, milestone checklist.

### Fixed

- CI workflow never ran: GitHub rejects `hashFiles()` in a job-level `if`.
  Replaced with step-level presence checks.
