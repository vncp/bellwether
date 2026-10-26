# Persisted-format fixture: storage version 1 specs on columns named with `,` or `@`

Two indexes written by the last storage version 1 binary, main at `c78ae6c`
(storage version 1, record version 2, encoding version 1), over one Parquet
file whose column names hold the separators of a key spec's identity
(`a,b`, `ts@x`, `id@ts`). Version 1 declared such specs without complaint, so
every later binary must read them. `crates/bellwether-cli/tests/format_compat.rs`
restores each and checks:

- `index-ab/`: `{"kind":"exact","columns":["a,b"]}`. It plans the lookup the
  version 1 binary planned, a no-op `index` leaves it at version 1, and the
  first build with work upgrades it to version 2.
- `index-collide/`: `{"kind":"temporal","columns":["id"],"ts_column":"ts@x"}`
  and `{"kind":"temporal","columns":["id@ts"],"ts_column":"x"}`, whose
  identities are both `temporal:id@ts@x`. It reads, `index` refuses to upgrade
  it without reading or writing anything and names the step, and following
  that step (re-declaring `temporal:id` with the time column `ts`) upgrades it.

Never edit these files. A format change gets a new fixture directory.

## How it was built

`data/part-0.parquet` holds 12 rows in three row groups of four: `a,b`, `id`
and `id@ts` are `row / 4` (int64), and `ts`, `ts@x` and `x` are timestamps
(ms) one, two and three hours apart from 2026-10-01. Its mtime was set to
2026-10-25T00:00:00Z (the fixture clock; a fingerprint's etag is the mtime)
before indexing, under the root `/private/tmp/b1r2/fx/data`, with that
commit's debug binary:

```
bellwether --clock 2026-10-25T00:00:00Z index data \
    --key '{"kind":"exact","columns":["a,b"]}'
bellwether --clock 2026-10-25T00:00:00Z index data --index collide \
    --key '{"kind":"temporal","columns":["id"],"ts_column":"ts@x"}' \
    --key '{"kind":"temporal","columns":["id@ts"],"ts_column":"x"}'
```

Each `index-*` directory is that index folder's `manifest.json`,
`manifests/` and `segments/`. `keys-ab.parquet` holds the key 1 in a column
`a,b`, and `keys-id.parquet` the key 2 in a column `id`. The version 1
binary planned `locate data --keys keys-ab.parquet` as 1 file, 1 row group,
656 bytes, and `latest data --index collide --keys keys-id.parquet` as 1
file, 1 row group, 657 bytes (`expected.json`).
