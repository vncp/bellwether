# Contributing

Bellwether is built against a fixed architecture. Read `docs/architecture.md` and the decision records in `docs/adr/` before changing anything; a pull request that contradicts an ADR needs a new ADR, not a comment.

## Rules

- **Types first.** `bellwether-types` is the contract. Changing it is an architectural decision and needs an ADR.
- **Invariants have tests.** Each invariant in `docs/invariants.md` names the test that enforces it. A change that touches an invariant updates the test in the same PR.
- **No estimates.** Costs are computed from statistics by replay. A number that is estimated is labeled as such in the type, never silently.
- **No collect-to-driver.** Memory is bounded per operation (ADR 018). A PR that materializes an unbounded set is rejected.
- **The agent proposes, the engine scores.** Nothing in `observe.*` or `propose.*` may write anything but a Proposal. Nothing in `apply.*` may run without a passing Certificate, and every successful apply writes a Receipt.
- **Format changes bump a version** and ship a migration and a fixture.
- **Layering is enforced.** `dev/check-layering.py` fails CI on any crate dependency edge not listed in it; adding an edge is an architectural decision. Supply chain is checked by `cargo deny` (`deny.toml`); rustdoc warnings are denied; mutation testing runs weekly and its survivors are published.
- **CI must be green**: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features` with warnings denied, `cargo test --workspace`.

## Pull requests

One crate per PR where possible. The description states which work package it implements, which invariants and ADRs it touches, and how it was tested. A fresh reviewer checks it against those before merge.

## License

By contributing you agree your contribution is licensed under the terms in `LICENSE.md`.
