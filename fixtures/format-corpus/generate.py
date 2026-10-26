"""Regenerates the data files of the persisted-format fixtures.

Run from the repository root: python3 fixtures/format-corpus/generate.py

`fixtures/format-v1/` and `fixtures/format-v0.1/` index copies of this corpus
(see their README.md). The corpus is a hive-partitioned folder of Parquet
files with a key column holding nulls and repeats, a symbol column, a
microsecond UTC timestamp, the two clocks of a bitemporal key, and a decimal
amount, written in row groups of four rows so every key spans several row
groups. `data/` holds the first two days; `extra/` holds the third day, which
the fixture's second commit adds. `keys-symbol.parquet` holds the keys of
the recorded workload, `keys-id.parquet` the keys the pinned lookups ask for, `right/` the same rows as `data/` with one amount changed
(the failing certificate's right side).

Written with pyarrow 23. The fixture tests never regenerate the files; they
read the committed bytes, so the bytes need not be reproducible.
"""

import datetime
import decimal
import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

OUT = os.path.dirname(os.path.abspath(__file__))
UTC = datetime.timezone.utc
DAY0 = datetime.datetime(2026, 1, 1, tzinfo=UTC)
SYMBOLS = ["AAA", "BBB", "CCC", None]

SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("symbol", pa.string()),
        ("ts", pa.timestamp("us", tz="UTC")),
        ("valid", pa.timestamp("us", tz="UTC")),
        ("known", pa.timestamp("us", tz="UTC")),
        ("amount", pa.decimal128(10, 2)),
    ]
)


def rows(day, part, n):
    out = []
    for i in range(n):
        seq = day * 100 + part * 20 + i
        key = None if seq % 11 == 5 else (seq * 7) % 9
        ts = DAY0 + datetime.timedelta(days=day, minutes=seq)
        valid = DAY0 + datetime.timedelta(days=day - (i % 2))
        known = ts + datetime.timedelta(hours=i % 3)
        out.append(
            {
                "id": key,
                "symbol": SYMBOLS[(day + 2 * part + i // 4) % 4],
                "ts": None if seq % 13 == 6 else ts,
                "valid": valid,
                "known": None if seq % 17 == 3 else known,
                "amount": decimal.Decimal(seq * 13 % 1000) / 4,
            }
        )
    return out


def write(path, records):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    table = pa.Table.from_pylist(records, schema=SCHEMA)
    pq.write_table(table, path, row_group_size=4)


def day_dir(base, day):
    return os.path.join(base, f"day=2026-01-0{day + 1}")


def main():
    for sub in ["data", "extra", "right"]:
        shutil.rmtree(os.path.join(OUT, sub), ignore_errors=True)
    for day in (0, 1):
        for part in (0, 1):
            records = rows(day, part, 10)
            write(os.path.join(day_dir(os.path.join(OUT, "data"), day), f"part-{part}.parquet"), records)
            if day == 1 and part == 1:
                records = [dict(r) for r in records]
                records[2]["amount"] += decimal.Decimal("0.01")
            write(os.path.join(day_dir(os.path.join(OUT, "right"), day), f"part-{part}.parquet"), records)
    write(os.path.join(day_dir(os.path.join(OUT, "extra"), 2), "part-0.parquet"), rows(2, 0, 10))
    ids = pa.table({"id": pa.array([1, 4, 7, 42], pa.int64())})
    pq.write_table(ids, os.path.join(OUT, "keys-id.parquet"))
    symbols = pa.table({"symbol": pa.array(["AAA", "CCC", "ZZZ"], pa.string())})
    pq.write_table(symbols, os.path.join(OUT, "keys-symbol.parquet"))


if __name__ == "__main__":
    main()
