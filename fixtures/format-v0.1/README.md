# Persisted-format fixture: storage version 2, record version 3 (M5, frozen)

**Status: M5. Storage version 2 and record version 3 are frozen (ADR 028).**
Every byte here is the frozen format: a later change to the bytes of a
segment, a manifest or a record is a new version with a new fixture
directory, never an edit of this one.

An index, its manifest history, and its ledger, plus the index of a CSV
folder, as written by the binary that froze storage version 2 (WP14/B1, PR 2;
ADR 028). Beside what `fixtures/format-v0.1-pre/` holds (per-key row counts,
per-row-group rows and bytes and per-column bytes in the manifest, segments
and completeness keyed by full spec identities), it holds every shape the
freeze added:

- per-key time bounds folded into the exact value record beside counts
  (`exact:id@ts,counts`: `bitmap | counts | bounds`, with the all-null
  sentinel where a key's times in a row group are all null);
- a bitemporal key spec with its own `bitemporal` segments
  (`temporal2:id@valid,known`: `bitmap | count | triples`);
- a bloom with per-key bounds (`bloom:id@0.01@ts`), whose `bounds` segment
  (a storage version 2 `keyed` segment under the bloom spec, each record
  `bitmap | bounds` and checksummed) is listed under the bloom's identity;
- a key proposal with bounds (`exact:symbol@ts,counts`) applied by `fix`,
  with its receipt;
- `schema.columns[].data_type` in core's `ColumnType` spelling and no
  `schema.sidecars`;
- in `index-text/`, the index of `fixtures/format-corpus/text/` (a CSV
  file), whose manifest holds `source.dialect`;
- in `index-nested/`, the index of `fixtures/format-corpus/nested/` (list,
  struct, map and dictionary columns, written by `nested.py`), whose
  manifest spells those columns `{"arrow": "<display>"}`.

Every later binary must read it: `crates/bellwether-cli/tests/format_compat.rs`
replays every pinned command over it and compares the output with
`expected.json` byte for byte, walks the ledger chain, and checks the shapes
above; `crates/bellwether-core/tests/counts_conformance.rs` checks that every
segment is named by its BLAKE3 and reads with the reader of its magic.

Never edit these files. A format change gets a new fixture directory.

## How it was built

From the branch's working tree, with its debug binary:

```
cargo build -p bellwether-cli
python3 fixtures/format-corpus/build_fixture.py \
    --bin target/debug/bellwether --out fixtures/format-v0.1 \
    --root /tmp/bellwether-format-v0.1 --m5
```

The steps are those of `fixtures/format-v0.1-pre/` (see
`fixtures/format-corpus/build_fixture.py`) with `--m5`: the index declares
`exact:id@ts,counts`, `bloom:id@0.01@ts`, `range:ts`,
`temporal:id@ts,counts` and `temporal2:id@valid,known`; the proposal `fix`
applies is `exact:symbol@ts,counts`; `--format csv index text --key
exact:id,counts` indexes the CSV folder; `index nested --key exact:id`
indexes the nested-column folder; and the pinned commands add `locate` and
`count` over the CSV folder and `locate` over the nested one. The Parquet data files are the ones the earlier
fixtures index (`generate.py` was not rerun). Everything ran under
`--clock 2026-10-25T00:00:00Z`, with every data file's mtime set to that
instant.
