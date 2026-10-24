## Work package

<!-- Which WP from docs/work-packages.md this implements, e.g. "WP1". -->

## What changed

<!-- One paragraph. What a reviewer should look at first. -->

## Invariants and ADRs touched

<!-- List each with one line on why it still holds. "None" is an answer. -->

- Invariants (docs/invariants.md):
- ADRs (docs/adr/):

## Testing

<!-- Which tests cover this. For an invariant, the test named in
docs/invariants.md is updated in this PR. -->

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace`

## Contract changes

<!-- Changes to bellwether-types, record schemas, or the on-disk format.
A type change needs an ADR. A format change bumps a version and ships a
migration and a fixture. -->

- [ ] No contract change
- [ ] Changes `bellwether-types` (ADR: )
- [ ] Changes the on-disk format or record schema (version bump, migration,
      fixture)
