"""Regenerates the files in fixtures/hints/ with pyarrow.

Run from the repository root: python3 fixtures/hints/generate.py

Each file carries an embedded Arrow schema (ARROW:schema) whose hints parquet
59 honours and parquet 53 did not; tests/legacy_inference.rs in the source
crate pins that the source still infers parquet 53's types from them.
Written with pyarrow 23; the bytes need not be reproducible, only the
physical types and the embedded schema.
"""

import decimal
import os

import pyarrow as pa
import pyarrow.parquet as pq

OUT = os.path.dirname(os.path.abspath(__file__))
N = 40
ids = list(range(N))

# INT64 columns carrying Duration hints, plus an ordinary Int64 control.
pq.write_table(
    pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "dur_ms": pa.array([i * 1000 for i in ids], pa.duration("ms")),
            "dur_s": pa.array([i * 7 for i in ids], pa.duration("s")),
            "dict_dur": pa.array([(i % 4) * 1000 for i in ids], pa.duration("ms")).dictionary_encode(),
        }
    ),
    os.path.join(OUT, "duration.parquet"),
    row_group_size=10,
)

# INT96 timestamps (deprecated encoding) written from several units and zones.
base = 1_700_000_000_000_000
pq.write_table(
    pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "ts96_us": pa.array([base + i for i in ids], pa.timestamp("us")),
            "ts96_ms_utc": pa.array([base // 1000 + i for i in ids], pa.timestamp("ms", "UTC")),
            "ts96_ns_utc": pa.array([base * 1000 + i for i in ids], pa.timestamp("ns", "UTC")),
            "ts96_ns": pa.array([base * 1000 + i for i in ids], pa.timestamp("ns")),
        }
    ),
    os.path.join(OUT, "int96.parquet"),
    row_group_size=10,
    use_deprecated_int96_timestamps=True,
)

# Narrow decimals, which parquet 59 can read as Decimal32 / Decimal64.
pq.write_table(
    pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "dec32": pa.array([decimal.Decimal(i) / 100 for i in ids], pa.decimal32(7, 2)),
            "dec64": pa.array([decimal.Decimal(i) / 1000 for i in ids], pa.decimal64(15, 3)),
            "dict_dec32": pa.DictionaryArray.from_arrays(
                pa.array([i % 4 for i in ids], pa.int32()),
                pa.array([decimal.Decimal(k) / 100 for k in range(4)], pa.decimal32(7, 2)),
            ),
        }
    ),
    os.path.join(OUT, "decimal.parquet"),
    row_group_size=10,
)
