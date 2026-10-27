"""Read a Bellwether scan plan in Spark.

`bellwether locate|latest|asof ... --emit spark --out plan.json` writes a
manifest; `read(spark, "plan.json")` returns the lookup's rows as a
DataFrame:

    from bellwether_plan import read
    df = read(spark, "plan.json")

What it does, exactly:

- reads the planned files (only those) with the manifest's schema;
- keeps, in a file whose plan reads only some of its row groups, the rows
  of those row groups (`_metadata.row_index` ranges from the index's row
  counts), so the rows are exactly the plan's;
- adds partition columns from the index's own reading of each path;
- applies the residual stage by stage, in the plan's order: key
  membership (a queried NaN by `isnan`, which Spark never pushes into the
  Parquet reader, whose statistics leave NaN out), ranges, list
  membership, the per-key lower bound (`not_older_than`, a left join on
  the key), the newest rows per key (`latest_per_key`: every row at the
  key's greatest timestamp, ties kept, a null key or timestamp never
  winning), and the live-row test;
- with `ordered`, returns rows in the plan's read order.

File level only: Spark opens the planned files and no other, but prunes
or reads their row groups by its own statistics; this shim pins none.
Every literal is built from typed JSON with Spark's typed constructors;
no SQL text is assembled from the manifest.
"""

import datetime
import json
import math
import struct
from decimal import Decimal
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql import Window

FORMAT = "bellwether.spark/1"
_POS = "__bellwether_pos"
_ROW = "__bellwether_row"
_BOUND = "__bellwether_bound"
_RANK = "__bellwether_rank"


class PlanError(ValueError):
    """The manifest is not one this shim reads."""


def _scale(unscaled, scale):
    # Decimal's constructor is exact; arithmetic (scaleb) would round to
    # the context's 28 digits.
    return Decimal(f"{int(unscaled)}E{-int(scale)}")


def _float(bits):
    return struct.unpack(">d", bytes.fromhex(bits))[0]


def _micros(value, unit):
    v = int(value)
    if unit == "s":
        return v * 1_000_000
    if unit == "ms":
        return v * 1_000
    if unit == "us":
        return v
    raise PlanError(f"a timestamp in unit {unit!r} has no exact Spark literal")


def literal(value, spark_type):
    """A Spark column literal of `value` (the manifest's typed JSON) at the
    column's Spark type `spark_type`, exact for every type the manifest
    carries."""
    t = value["type"]
    if t == "boolean":
        return F.lit(bool(value["value"]))
    if t in ("int8", "int16", "int32", "int64", "uint8", "uint16", "uint32"):
        return F.lit(int(value["value"])).cast(spark_type)
    if t == "uint64":
        return F.lit(Decimal(int(value["value"]))).cast(spark_type)
    if t in ("float32", "float64"):
        return F.lit(_float(value["bits"])).cast(spark_type)
    if t == "decimal":
        return F.lit(_scale(value["unscaled"], value["scale"])).cast(spark_type)
    if t == "utf8":
        return F.lit(value["value"])
    if t == "binary":
        return F.lit(bytearray.fromhex(value["hex"]))
    if t == "date32":
        return F.date_from_unix_date(F.lit(int(value["days"])))
    if t == "date64":
        ms = int(value["ms"])
        if ms % 86_400_000:
            raise PlanError("a Date64 value that is not a whole day")
        return F.date_from_unix_date(F.lit(ms // 86_400_000))
    if t == "timestamp":
        us = _micros(value["value"], value["unit"])
        if value.get("tz") is not None:
            return F.timestamp_micros(F.lit(us))
        # A wall-clock timestamp (no zone): built from its fields, so no
        # session time zone takes part.
        day, rem = divmod(us, 86_400_000_000)
        d = datetime.date(1970, 1, 1) + datetime.timedelta(days=day)
        hour, rem = divmod(rem, 3_600_000_000)
        minute, rem = divmod(rem, 60_000_000)
        secs = _scale(rem, 6)
        return F.make_timestamp_ntz(
            F.lit(d.year),
            F.lit(d.month),
            F.lit(d.day),
            F.lit(hour),
            F.lit(minute),
            F.lit(secs).cast("decimal(8,6)"),
        )
    raise PlanError(f"no Spark literal for a {t} value")


def _col(name):
    return F.col("`" + name.replace("`", "``") + "`")


def _null_or(value, spark_type):
    if value is None:
        return F.lit(None).cast(spark_type)
    return literal(value, spark_type)


def _read_file(spark, m, i, f, single_schema):
    df = spark.read.schema(single_schema).parquet(f["path"])
    rows = f.get("rows")
    meta = m["_needs_meta"]
    if meta:
        df = df.select("*", F.col("_metadata.row_index").alias(_ROW))
    if rows is not None:
        if rows:
            test = reduce(
                lambda a, b: a | b,
                [F.col(_ROW).between(int(lo), int(hi)) for lo, hi in rows],
            )
        else:
            test = F.lit(False)
        df = df.where(test)
    for c in m["partition_columns"]:
        df = df.withColumn(c, _null_or(f["partition_values"].get(c), m["types"][c]))
    if m["ordered"]:
        pos = None
        for p, step in enumerate(m["read_order"]):
            if step["file"] != i:
                continue
            if "rows" in step:
                lo, hi = step["rows"]
                cond = F.col(_ROW).between(int(lo), int(hi))
            else:
                cond = F.lit(True)
            pos = F.when(cond, F.lit(p)) if pos is None else pos.when(cond, F.lit(p))
        df = df.withColumn(_POS, pos if pos is not None else F.lit(None).cast("int"))
    return df


def _key(name, m):
    """A key column as Spark should group and compare it: a float column
    also by its sign, because Spark folds -0.0 into +0.0 and the plan does
    not."""
    c = _col(name)
    if name in m["float_columns"]:
        return [c, c.cast("string")]
    return [c]


def _stage(spark, df, s, m):
    op = s["op"]
    types = m["types"]
    if op == "in":
        cols = s["columns"]
        rows = s["rows"]
        if m["keys_file"] is not None and m["_keys_used"] is False:
            m["_keys_used"] = True
            keys = spark.read.parquet(m["keys_file"]).select(*[_col(c) for c in cols])
            return _semi(df, keys, cols)
        if not rows:
            return df.where(F.lit(False))
        if len(cols) == 1 and len(rows) <= 1024:
            c = cols[0]
            nan = any(_is_nan(r[0]) for r in rows)
            vals = [literal(r[0], types[c]) for r in rows if not _is_nan(r[0])]
            test = _col(c).isin(vals) if vals else F.lit(False)
            if nan:
                # A queried NaN is matched by isnan(), which Spark never
                # pushes into the Parquet reader: the reader's min/max
                # statistics leave NaN out and would drop its row group.
                test = test | F.isnan(_col(c))
            return df.where(test)
        keys = _relation(spark, [[(c, v, types[c]) for c, v in zip(cols, r)] for r in rows])
        return _semi(df, keys, cols)
    if op == "between":
        c = s["column"]
        return df.where(
            _col(c).between(literal(s["lo"], types[c]), literal(s["hi"], types[c]))
        )
    if op == "contains_any":
        c = s["column"]
        if not s["values"]:
            return df.where(F.lit(False))
        lst = F.map_keys(_col(c)) if s["map"] else _col(c)
        et = _element_type(types[c], s["map"])
        return df.where(
            _col(c).isNotNull()
            & F.arrays_overlap(lst, F.array(*[literal(v, et) for v in s["values"]]))
        )
    if op == "contains_all":
        c = s["column"]
        lst = F.map_keys(_col(c)) if s["map"] else _col(c)
        et = _element_type(types[c], s["map"])
        test = _col(c).isNotNull()
        for g in s["groups"]:
            test = test & F.coalesce(
                F.arrays_overlap(lst, F.array(*[literal(v, et) for v in g])), F.lit(False)
            )
        return df.where(test)
    if op == "live":
        c = s["column"]
        return df.where(_col(c).isNotNull() & (_col(c) != literal(s["tombstone"], types[c])))
    if op == "not_older_than":
        cols = s["columns"]
        ts = s["ts_column"]
        if not s["bounds"]:
            return df
        bounds = _relation(
            spark,
            [
                [(c, k, types[c]) for c, k in zip(cols, b["key"])] + [(_BOUND, b["ts"], types[ts])]
                for b in s["bounds"]
            ],
        ).alias("b")
        left = df.alias("s")
        cond = reduce(
            lambda a, b: a & b,
            [F.col("s." + _q(c)) == F.col("b." + _q(c)) for c in cols],
        )
        joined = left.join(bounds, cond, "left")
        kept = joined.where(
            F.col("b." + _BOUND).isNull() | (F.col("s." + _q(ts)) >= F.col("b." + _BOUND))
        )
        return kept.select("s.*")
    if op == "latest_per_key":
        cols = s["columns"]
        ts = s["ts_column"]
        not_null = reduce(lambda a, b: a & b, [_col(c).isNotNull() for c in cols + [ts]])
        keys = [k for c in cols for k in _key(c, m)]
        w = Window.partitionBy(*keys).orderBy(_col(ts).desc())
        return (
            df.where(not_null)
            .withColumn(_RANK, F.rank().over(w))
            .where(F.col(_RANK) == 1)
            .drop(_RANK)
        )
    raise PlanError(f"the residual stage {op!r} is not one this shim runs")


def _q(name):
    return "`" + name.replace("`", "``") + "`"


def _is_nan(value):
    return value is not None and value["type"] in ("float32", "float64") and math.isnan(
        _float(value["bits"])
    )


def _relation(spark, rows):
    """A DataFrame of `rows`, each a list of `(column, typed JSON value or
    None, Spark type)`: one array of typed structs, inlined, so a long
    list is one plan node, not a union per row."""
    names = [c for c, _, _ in rows[0]]
    structs = [
        F.struct(*[_null_or(v, t).alias(c) for c, v, t in row]) for row in rows
    ]
    return spark.range(1).select(F.inline(F.array(*structs))).select(*[_col(c) for c in names])


def _semi(df, keys, cols):
    left = df.alias("s")
    right = keys.alias("k")
    cond = reduce(
        lambda a, b: a & b,
        [F.col("s." + _q(c)) == F.col("k." + _q(c)) for c in cols],
    )
    return left.join(right, cond, "left_semi")


def _element_type(spark_type, is_map):
    t = spark_type.strip()
    if is_map:
        inner = t[len("map<") : -1]
        depth = 0
        for i, ch in enumerate(inner):
            if ch == "<" or ch == "(":
                depth += 1
            elif ch == ">" or ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                return inner[:i]
        raise PlanError(f"cannot read the key type of {t}")
    return t[len("array<") : -1]


def read(spark, manifest_path):
    """The plan's rows, exactly: the planned files, the planned row groups'
    rows, and the residual in order (see the module docstring)."""
    with open(manifest_path, encoding="utf-8") as fh:
        m = json.load(fh)
    if m.get("format") != FORMAT:
        raise PlanError(
            f"{manifest_path} is not a {FORMAT} manifest (got {m.get('format')!r}); write one with "
            "`bellwether locate|latest|asof ... --emit spark --out <path>`"
        )
    m["_keys_used"] = False
    files = m["files"]
    needs_meta = m["ordered"] or any(f.get("rows") is not None for f in files)
    m["_needs_meta"] = needs_meta
    if not files:
        schema = ", ".join(f"{_q(c)} {m['types'][c]}" for c in m["columns"])
        return spark.createDataFrame([], schema)
    single = m["read_schema"]
    per_file = needs_meta or m["partition_columns"]
    if per_file:
        parts = [_read_file(spark, m, i, f, single) for i, f in enumerate(files)]
        df = reduce(lambda a, b: a.unionByName(b), parts)
    else:
        df = spark.read.schema(single).parquet(*[f["path"] for f in files])
    for s in m["residual"]["stages"]:
        df = _stage(spark, df, s, m)
    if m["ordered"]:
        df = df.orderBy(F.col(_POS), F.col(_ROW))
    return df.select(*[_col(c) for c in m["columns"]])
