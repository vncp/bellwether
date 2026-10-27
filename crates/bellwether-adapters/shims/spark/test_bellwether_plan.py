"""Unit tests of the Spark shim's literals and refusals (`bellwether_plan`).

Run with `python -m pytest shims/spark/test_bellwether_plan.py`; skipped,
with the reason, when `pyspark` does not import. The end-to-end tests
(every lookup shape against the reference evaluator) are
`tests/spark.rs`, which also runs this file.
"""

import json
import math
import os
import struct
import sys

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark does not import; the shim needs it")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bellwether_plan import PlanError, literal, read  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("bellwether-shim-pytest")
        .config("spark.ui.enabled", "false")
        # A session zone far from UTC: no literal may depend on it.
        .config("spark.sql.session.timeZone", "America/St_Johns")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def one(spark, col):
    return spark.range(1).select(col.alias("v")).collect()[0]["v"]


def bits(f):
    return struct.pack(">d", f).hex()


def test_float_literals_keep_their_bits(spark):
    neg = one(spark, literal({"type": "float64", "bits": bits(-0.0)}, "double"))
    assert math.copysign(1.0, neg) == -1.0
    assert math.isnan(one(spark, literal({"type": "float64", "bits": bits(math.nan)}, "double")))
    assert one(spark, literal({"type": "float64", "bits": bits(0.1)}, "double")) == 0.1


def test_decimals_are_exact_at_their_scale(spark):
    from decimal import Decimal

    v = one(spark, literal({"type": "decimal", "precision": 38, "scale": 4, "unscaled": "-123456789012345678901234567890123456"}, "decimal(38,4)"))
    assert v == Decimal("-12345678901234567890123456789012.3456")


def test_timestamps_do_not_depend_on_the_session_zone(spark):
    import datetime

    ntz = one(spark, literal({"type": "timestamp", "unit": "ms", "tz": None, "value": "1700000000997"}, "timestamp_ntz"))
    assert ntz == datetime.datetime(2023, 11, 14, 22, 13, 20, 997000)
    # An instant: the same UTC microsecond whatever the session zone.
    utc = spark.range(1).select(
        literal({"type": "timestamp", "unit": "us", "tz": "UTC", "value": "1704067200000001"}, "timestamp").cast("long").alias("s"),
    ).collect()[0]["s"]
    assert utc == 1704067200
    with pytest.raises(PlanError):
        literal({"type": "timestamp", "unit": "ns", "tz": None, "value": "1"}, "timestamp_ntz")


def test_strings_and_binary_are_values_not_text(spark):
    hostile = "o'k\"; DROP TABLE t; --\\ \u00df \x00"
    assert one(spark, literal({"type": "utf8", "value": hostile}, "string")) == hostile
    assert bytes(one(spark, literal({"type": "binary", "hex": "00ff27"}, "binary"))) == b"\x00\xff'"


def test_unsigned_64_bit_reads_as_decimal(spark):
    from decimal import Decimal

    v = one(spark, literal({"type": "uint64", "value": "18446744073709551615"}, "decimal(20,0)"))
    assert v == Decimal(18446744073709551615)


def test_a_manifest_of_another_format_is_refused(spark, tmp_path):
    p = tmp_path / "m.json"
    p.write_text(json.dumps({"format": "other/1"}))
    with pytest.raises(PlanError, match="bellwether.spark/1"):
        read(spark, str(p))


def test_an_unknown_stage_is_refused(spark):
    from bellwether_plan import _stage

    with pytest.raises(PlanError, match="drop_everything"):
        _stage(spark, spark.range(1), {"op": "drop_everything"}, {"types": {}})


def test_a_plan_of_no_files_is_the_typed_empty_result(spark, tmp_path):
    p = tmp_path / "m.json"
    p.write_text(
        json.dumps(
            {
                "format": "bellwether.spark/1",
                "files": [],
                "columns": ["a"],
                "types": {"a": "bigint"},
                "ordered": False,
                "partition_columns": [],
                "read_schema": "`a` bigint",
                "residual": {"mode": "full", "stages": []},
                "keys_file": None,
            }
        )
    )
    df = read(spark, str(p))
    assert df.collect() == []
    assert df.schema.simpleString() == "struct<a:bigint>"
