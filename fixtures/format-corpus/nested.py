"""Writes `nested/part-0.parquet`: the folder whose index pins, in the M5
fixture (`fixtures/format-v0.1/index-nested/`), the manifest spelling of
types core's `ColumnType` has no form for, `{"arrow": "<display>"}`
(`docs/format.md`, "Manifest column types"): a list, a struct, a map and a
dictionary column beside the `id` key and a timestamp.

Two row groups of three rows. Run from the repository root:

    python3 fixtures/format-corpus/nested.py

The file is checked in; this script is how it was written (pyarrow 23).
"""

import os

import pyarrow as pa
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))

schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("ts", pa.timestamp("us", tz="UTC")),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("attrs", pa.struct([("venue", pa.string()), ("lot", pa.int32())])),
        pa.field("props", pa.map_(pa.string(), pa.int64())),
        pa.field("side", pa.dictionary(pa.int32(), pa.string())),
    ]
)
DAY_US = 86_400_000_000
BASE = 20_454 * DAY_US  # 2026-01-01T00:00:00Z
table = pa.table(
    {
        "id": [1, 2, 3, 2, 3, 4],
        "ts": [BASE + i * 3_600_000_000 for i in range(6)],
        "tags": [["a"], ["a", "b"], [], None, ["c"], ["b", "c"]],
        "attrs": [
            {"venue": "X", "lot": 100},
            {"venue": "Y", "lot": 200},
            None,
            {"venue": "X", "lot": None},
            {"venue": "Z", "lot": 50},
            {"venue": "Y", "lot": 10},
        ],
        "props": [[("k", 1)], [], None, [("k", 2), ("m", 3)], [("m", 4)], [("k", 5)]],
        "side": pa.array(["buy", "sell", "buy", None, "sell", "buy"]).dictionary_encode(),
    },
    schema=schema,
)
out = os.path.join(HERE, "nested")
os.makedirs(out, exist_ok=True)
pq.write_table(table, os.path.join(out, "part-0.parquet"), row_group_size=3)
print(os.path.join(out, "part-0.parquet"))
