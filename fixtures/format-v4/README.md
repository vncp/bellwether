# Persisted-format fixture: storage version 2, record version 4 (B8 tombstones)

Storage version 2 is the frozen M5 format, unchanged; record version 4 is
the version B8 tombstones bring to ledger records (ADR 028, "Records after
the freeze"). Every byte here is the format a later binary must read: a
change to the bytes of a segment, a manifest or a record is a new version
with a new fixture directory, never an edit of this one.

An index of `fixtures/format-corpus/live/` (an order ledger with
retractions, written by `fixtures/format-corpus/live.py`), its manifest
history, its ledger, and the recorded workload with its key set
(`index/cache/`, objects named by their hashes). It holds:

- a bitemporal key with a value tombstone,
  `temporal2:id@valid,known,tombstone=status=CANCEL`, and a temporal key
  with a boolean tombstone, `temporal:id@ts,tombstone=void`, each declared
  in `schema.key_specs` without its tombstone and with a `tombstone`
  sidecar and its `keyed_tombstone` segments (storage version 2, no new
  bytes);
- a live chain, every record at `record_version` 4: `why --latest-by ts
  --tombstone void` (profile Evidence; engine Proposals and Replays; a
  workload of live `latest` runs, `RecordedRun.tombstone`), `propose --key
  temporal:id@ts,tombstone=void` (a Proposal whose key spec carries a
  tombstone), `fix --verify-only` (Replay, plan Evidence, a Certificate
  whose row filter ends in `live`), and `fix --certificate` (the apply
  tier: the Receipt);
- the plan Evidence of every pinned lookup: `latest` and `asof --at` with
  and without `--live`, the bitemporal `asof --valid --known --live` and
  `latest --known --live`, and `locate` (`--live` residuals end in `live`).

`crates/bellwether-cli/tests/format_compat.rs`
(`the_record_version_4_fixture_reads_byte_for_byte`) restores it under a
fresh root, walks the ledger chain (`receipt show` prints the pinned text,
every record is named by its hash and is version 4), replays every pinned
lookup and compares it with `expected.json` byte for byte, and checks the
shapes above.

Never edit these files. A format change gets a new fixture directory.

## How it was built

From the branch's working tree, with its debug binary:

```
cargo build -p bellwether-cli
python3 fixtures/format-corpus/live.py
python3 fixtures/format-corpus/build_live_fixture.py \
    --bin target/debug/bellwether --out fixtures/format-v4 \
    --root /tmp/bellwether-format-v4
```

The steps are in `fixtures/format-corpus/build_live_fixture.py`. Everything
ran under `--clock 2026-10-25T00:00:00Z`, with every data file's mtime set
to that instant.
