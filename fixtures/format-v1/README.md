# Persisted-format fixture: storage version 1, record version 2

An index, its manifest history, and its ledger as written by the last binary
before storage version 2: main at `495682a` (storage version 1, record
version 2, encoding version 1; per-key time bounds and bitemporal versions as
sidecar segments). Every later binary must read it; `crates/bellwether/tests/format_compat.rs`
plans every pinned lookup over it and compares the output with
`expected.json` byte for byte, walks the ledger chain, and commits a copy plus
one new file at the current storage version with every old plan unchanged.

Never edit these files. A format change gets a new fixture directory.

## How it was built

From a clean worktree at `495682a`, with that commit's debug binary:

```
cargo build -p bellwether-cli
python3 fixtures/format-corpus/generate.py
python3 fixtures/format-corpus/build_fixture.py \
    --bin target/debug/bellwether --out fixtures/format-v1 --root /tmp/bellwether-format-v1
```

`fixtures/format-corpus/build_fixture.py` lists the steps: a hive-partitioned
index with `exact:id@ts`, `bloom:id@0.01`, `range:ts`, `temporal:id@ts`, and
`temporal2:id@valid,known` (sequence 1); one more day of data (sequence 2);
`why`, `propose --key exact:symbol`, and `fix` (sequence 3, a receipt with its
full chain); a failing `reconcile` against `fixtures/format-corpus/right/`;
then every pinned lookup with `--json` and `receipt show`. Everything ran
under `--clock 2026-10-25T00:00:00Z`, and every data file's mtime was set to
that instant, so a file's fingerprint (its etag is the mtime) is the same
wherever the fixture is restored.

`index/` is the index folder (`.bellwether`) without its rebuildable cache.
`expected.json` holds the root the index was built under (`root`), the steps,
the hashes of the receipt, the proposal, the recorded workload, and the
failing certificate, every pinned lookup's arguments (`<corpus>` stands for
`fixtures/format-corpus`) and output lines, the `receipt show` text, and the
hash of every ledger record. A restore copies `fixtures/format-corpus/data/`
and `extra/` under a new root and replaces the recorded root with it in the
manifests and the pinned outputs.
