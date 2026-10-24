# Changelog

All notable changes are documented here. The format follows Keep a Changelog; versions follow Semantic Versioning. Until 1.0, the on-disk format version in `bellwether-types::format` is the compatibility contract, not the crate version.

## [Unreleased]

### Added

- `bellwether` (the facade, WP9): one `Engine` whose operations (`why`, `index`, `locate`, `latest`, `asof`, `propose`, `reconcile`, `fix`, `receipt`, `report`) return typed results that are the CLI's `--json` output and what the MCP server returns (ADR 010).
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
- `docs/format.md`: dataset-schema scale and unit for decimals and
  timestamps, statistics semantics, per-kind segment layouts, `bloom_hash`
  header field, pinned encoding test vectors.
- README rewritten: tagline, loop diagram, highlights, comparison table,
  what the project deliberately is not, milestone checklist.

### Fixed

- CI workflow never ran: GitHub rejects `hashFiles()` in a job-level `if`.
  Replaced with step-level presence checks.
