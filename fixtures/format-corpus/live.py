"""Writes `live/`: the folder whose index and ledger `fixtures/format-v4/`
holds (record version 4, B8 tombstones): an order ledger of retractions.

`live/data/part-{0,1,2}.parquet` hold 8 rows each in row groups of 4. Row
`k` (0..24) has `id` `k % 6` (null when `k == 19`), `ts` at minute `k` of
2026-01-01 (null when `k == 13`), `valid` the day of `k // 12`, `known`
`ts` plus `k % 3` hours, `void` true when `k % 5 == 3` (null when
`k == 22`), and `status` `CANCEL` where `void` is true, `OPEN` otherwise
(null where `void` is null). So some keys' newest row is a retraction,
one key is retracted and written again, and a null tombstone column is not
live. `live/keys.parquet` holds the ids the workload and the pinned lookups
ask for: 0 to 6 (6 has no row).

Run from the repository root:

    python3 fixtures/format-corpus/live.py

The files are checked in; this script is how they were written (pyarrow 23).
The fixture tests never regenerate them.
"""

import datetime
import os

import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "live")
UTC = datetime.timezone.utc
DAY0 = datetime.datetime(2026, 1, 1, tzinfo=UTC)

SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("ts", pa.timestamp("us", tz="UTC")),
        ("valid", pa.timestamp("us", tz="UTC")),
        ("known", pa.timestamp("us", tz="UTC")),
        ("void", pa.bool_()),
        ("status", pa.string()),
    ]
)


def row(k):
    ts = DAY0 + datetime.timedelta(minutes=k)
    void = None if k == 22 else k % 5 == 3
    return {
        "id": None if k == 19 else k % 6,
        "ts": None if k == 13 else ts,
        "valid": DAY0 + datetime.timedelta(days=k // 12),
        "known": ts + datetime.timedelta(hours=k % 3),
        "void": void,
        "status": None if void is None else "CANCEL" if void else "OPEN",
    }


def main():
    data = os.path.join(HERE, "data")
    os.makedirs(data, exist_ok=True)
    for part in range(3):
        rows = [row(k) for k in range(part * 8, part * 8 + 8)]
        table = pa.Table.from_pylist(rows, schema=SCHEMA)
        pq.write_table(table, os.path.join(data, f"part-{part}.parquet"), row_group_size=4)
    keys = pa.table({"id": pa.array(list(range(7)), pa.int64())})
    pq.write_table(keys, os.path.join(HERE, "keys.parquet"))


if __name__ == "__main__":
    main()
