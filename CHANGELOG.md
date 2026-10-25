# Changelog

All notable changes are documented here. The format follows Keep a Changelog; versions follow Semantic Versioning. Until 1.0, the on-disk format version in `bellwether-types::format` is the compatibility contract, not the crate version.

## [Unreleased]

### Added

- README rewritten product-first: a wordmark, one measured line from the Results under the promise, what you can do with it, the `why` capture in "In one minute", a thirty-second start, how it works, the Results unchanged, then one section per capability, each paired with a real terminal capture (`fix` and a failing `reconcile`, `report`, bitemporal `asof`, an MCP refusal over stdio); each idea is stated once. `scripts/readme-captures.sh` generates its data with `bellwether bench`, runs every command shown in a pseudo-terminal, and stores the text beside each image; `scripts/render_terminal.py` draws the captures and `scripts/render_figures.py` the wordmark, loop, and architecture figures, both in light and dark. CI fails when a capture or figure no longer matches its committed source. The loop figure moves to `docs/assets/readme/` and the three-step figure is removed (it repeated the numbered steps); the evolve design is summarized in one paragraph linking `docs/evolve.md`.
- README "Results": realistic questions on NYC taxi, GH Archive, ClickBench, and a synthetic market corpus, each answered by a full scan, DuckDB alone, and DuckDB on Bellwether's plan after `why` → `fix`, plus `reconcile` against DuckDB `EXCEPT ALL` (`scripts/showcase.sh`). Every chart and table is rendered from `docs/measured/results/*.json` by `scripts/render_charts.py` (schema in `docs/measured/results/README.md`), and CI fails when a committed chart or table no longer matches the committed results. The ClickBench transcript moves from the README to `docs/measured/2026-10-24-clickbench.md`; downloads are shared with `scripts/realdata.sh` through `scripts/datasets.sh`.
- Signed receipts (WP19, ADR 029): `Ledger::sign`, `signatures`, `verify_against`, and `explain_verified` over detached ed25519 objects `ledger/<hash>.sig.<key id>.json`; `bellwether keys generate|show|trust`; `fix --signing-key` (or `BELLWETHER_SIGNING_KEY`, a path) signs the receipt and certificate it wrote; `--trusted-keys` (or `BELLWETHER_TRUSTED_KEYS`) on `receipt show`, `ledger receipts`, and `report` gives verdicts (`signed`, `untrusted`, `unsigned`, `invalid`); `receipt show --require-signature` exits 1 unless every link is intact, the receipt and its certificate are signed by a trusted key, and the evidence is about this data root (signatures bind content, not location); key and trust files inside the index location are refused; the MCP server takes `--signing-key` and `--trusted-keys`, and each envelope chain carries `signatures` and `verdict`. No record or storage version changes.
- Release measurements (WP22a): `scripts/perf.sh` (with `scripts/perf.py`, `perf-report.py`, `perf-budgets.py`) measures lookups against a full scan and DuckDB, build throughput by thread count, reconcile throughput by memory budget, replay and observe latency, and resident bytes per key, profiles the hot paths with samply, and writes `benches/out/perf.json` (`bellwether-perf/1`); published in `docs/measured/2026-10-25-performance.md` with `docs/measured/budgets.toml` and the chart rows `docs/measured/results/perf.json`. `bellwether index --threads N` sets the build's worker count (the segments written do not depend on it); `bellwether bench read --plan|--all` reads planned row groups through the benchmark's byte-counting reader.
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
- `docs/format.md`: dataset-schema scale and unit for decimals and
  timestamps, statistics semantics, per-kind segment layouts, `bloom_hash`
  header field, pinned encoding test vectors.
- README rewritten: tagline, loop diagram, highlights, comparison table,
  what the project deliberately is not, milestone checklist.

### Fixed

- `fix` built a new index without the folder's partitioning, so a later
  `locate --partitioning hive` (or `--path-template`) was refused as "built
  with partitioning none". `index` and `fix` now share one set of build
  options (`Engine::build_options`), which records the scheme in
  `manifest.source`.
- `propose --key temporal2:id@valid,known` (and `exact:a@ts`) failed with
  "unknown kind". `propose` now reads `--key` as `index --key` does, and
  refuses a sidecar with a typed refusal (exit 3, MCP
  `sidecar_not_proposable`) that names the `index` command that builds it.
  MCP `propose.key` also takes the short form.
- `--chunk-size` on a Parquet folder was accepted by `why` and `propose`
  and refused by `index` and `fix`; every verb now refuses it as a usage
  error (exit 2) naming `--format csv|jsonl`.
- CI workflow never ran: GitHub rejects `hashFiles()` in a job-level `if`.
  Replaced with step-level presence checks.
