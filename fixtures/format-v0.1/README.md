# Persisted-format fixture: storage version 2, record version 3

An index, its manifest history, and its ledger as written by the first
binary at storage version 2 and record version 3 (WP14, PR 1; ADR 028):
per-key row counts in exact and temporal segments, per-row-group rows and
bytes and per-column bytes in the manifest, segments and completeness keyed
by full spec identities. Every later binary must read it:
`crates/bellwether-cli/tests/format_compat.rs` replays every pinned command
over it and compares the output with `expected.json` byte for byte, and
walks the ledger chain.

Never edit these files. A format change gets a new fixture directory.

## How it was built

From the branch's working tree, with its debug binary:

```
cargo build -p bellwether-cli
python3 fixtures/format-corpus/generate.py
python3 fixtures/format-corpus/build_fixture.py \
    --bin target/debug/bellwether --out fixtures/format-v0.1 \
    --root /tmp/bellwether-format-v0.1 --counts
```

The steps are those of `fixtures/format-v1/` (see
`fixtures/format-corpus/build_fixture.py`) with `--counts`: the temporal key
is declared `temporal:id@ts,counts`, the proposal `fix` applies is
`exact:symbol,counts`, and the pinned commands add `count` over both key
files and `locate --columns id,amount`. The per-key time bounds
(`exact:id@ts`) and the bitemporal key (`temporal2:id@valid,known`) are
still sidecar segments at this version, read as storage version 1 sidecars
are. Everything ran under `--clock 2026-10-25T00:00:00Z`, with every data
file's mtime set to that instant.
