#!/usr/bin/env python3
"""Measured funnel for per-key counts (storage version 2) on both corpus shapes.

The operator is the count pass that gates a query: `count(*) where key in
(...)` over a key set, before the query that reads those keys. Two corpora:
the versioned ledger (`bellwether bench generate`, key `entity_id`) and the
time-series `market` corpus (`bellwether bench market`, key `symbol`). Key
sets of 10, 10^3 and 10^5 distinct keys, drawn deterministically; the market
corpus has fewer symbols than the larger sets, so those sets add absent
symbols (a gate mostly says no), which the count answers as zero rows.

For each (corpus, key set) the table reports, from plans and counts the CLI
printed with --json (exact sums of statistics, never estimates):

  live              row groups and compressed bytes in the index
  locate            row groups and bytes a locate of the keys plans (exact:<key>)
  key column        bytes of only the key column over those row groups
                    (locate --columns <key>): what a count reads without counts
  with counts       data bytes a `count` reads under exact:<key>,counts: printed
                    as "0 by construction", not measured; the count reads the
                    segments and opens no data file (crates/bellwether-build/
                    tests/counts.rs, a_version_2_plan_opens_no_footer_...)
  rows              the count's rows, and the oracle's: every data file
                    decoded with pyarrow, rows whose key is in the set

Usage: scripts/counts-funnel.py --bin PATH/TO/bellwether [--work DIR]
       [--label "debug build"]

Needs python3 with pyarrow. Prints markdown; the label line names the
machine and the build (ADR 019). No timing is measured.
"""

import argparse
import json
import os
import platform
import random
import shutil
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

SIZES = [10, 1_000, 100_000]


def run(bin_, args):
    out = subprocess.run([bin_, *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"bellwether {' '.join(args)} failed ({out.returncode}): {out.stderr.strip()}")
    return out.stdout


def lookup(bin_, args):
    return json.loads(run(bin_, [*args, "--json"]))


def machine():
    cpu = platform.processor() or platform.machine()
    if sys.platform == "darwin":
        try:
            cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                 capture_output=True, text=True).stdout.strip() or cpu
        except OSError:
            pass
    return f"{platform.system()} {platform.release()} {platform.machine()}, {cpu}, {os.cpu_count()} cores"


def commit():
    repo = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    out = subprocess.run(["git", "-C", repo, "rev-parse", "--short", "HEAD"],
                         capture_output=True, text=True)
    return out.stdout.strip() or "unknown"


def key_sets(present, absent, rng):
    """Key sets of each size: present keys first (shuffled), then absent ones."""
    present = sorted(present)
    rng.shuffle(present)
    out = {}
    for n in SIZES:
        take = present[:n]
        extra = [absent(i) for i in range(n - len(take))]
        out[n] = take + extra
    return out


def write_keys(path, column, values, typ):
    pq.write_table(pa.table({column: pa.array(values, type=typ)}), path)


def oracle_rows(data, column, values, partitioning):
    dataset = ds.dataset(data, format="parquet", partitioning=partitioning)
    wanted = set(values)
    rows = 0
    for batch in dataset.to_batches(columns=[column]):
        rows += sum(1 for v in batch.column(0).to_pylist() if v in wanted)
    return rows


def corpus_rows(bin_, name, data, column, typ, values_by_n, work, extra):
    """Indexes `data` twice and prices every key set; returns the table rows."""
    plain = os.path.join(work, f"idx-{name}-exact")
    counted = os.path.join(work, f"idx-{name}-counts")
    for d in (plain, counted):
        shutil.rmtree(d, ignore_errors=True)
    lookup(bin_, ["index", data, "--index", plain, "--key", f"exact:{column}", *extra])
    lookup(bin_, ["index", data, "--index", counted, "--key", f"exact:{column},counts", *extra])
    rows = []
    for n, values in values_by_n.items():
        kf = os.path.join(work, f"keys-{name}-{n}.parquet")
        write_keys(kf, column, values, typ)
        base = ["--keys", kf, *extra]
        loc = lookup(bin_, ["locate", data, "--index", plain, *base])
        proj = lookup(bin_, ["locate", data, "--index", plain, "--columns", column, *base])
        cnt = lookup(bin_, ["count", data, "--index", counted, *base])
        with_rows = lookup(bin_, ["locate", data, "--index", counted, *base])
        partitioning = "hive" if "--partitioning" in extra else None
        want = oracle_rows(data, column, values, partitioning)
        assert cnt["rows"] == want, (name, n, cnt["rows"], want)
        assert with_rows.get("rows") == want, (name, n, with_rows.get("rows"), want)
        assert proj["planned"] == loc["planned"], (name, n)
        rows.append({
            "keys": n,
            "live_rg": loc["row_groups_total"],
            "locate_rg": loc["planned"]["row_groups"],
            "locate_bytes": loc["planned"]["bytes"],
            "key_bytes": proj["projected_bytes"],
            "count_rg": cnt["row_groups"],
            "rows": cnt["rows"],
            "oracle": want,
        })
    return rows


def table(title, rows, live_bytes):
    print(f"### {title}\n")
    print(f"Live compressed bytes in the index: {live_bytes:,}.\n")
    print("| keys | live row groups | locate: row groups | locate: bytes | count without counts: key-column bytes | count with counts: data bytes | rows (count) | rows (oracle) |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        print(f"| {r['keys']:,} | {r['live_rg']:,} | {r['locate_rg']:,} | {r['locate_bytes']:,} | "
              f"{r['key_bytes']:,} | 0 by construction | {r['rows']:,} | {r['oracle']:,} |")
    print()


def live_bytes(data, partitioning):
    total = 0
    for f in ds.dataset(data, format="parquet", partitioning=partitioning).files:
        md = pq.ParquetFile(f).metadata
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            total += sum(rg.column(c).total_compressed_size for c in range(rg.num_columns))
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work")
    ap.add_argument("--label", default="debug build")
    a = ap.parse_args()
    work = os.path.realpath(a.work or tempfile.mkdtemp(prefix="bw-counts-"))
    os.makedirs(work, exist_ok=True)
    rng = random.Random(14)

    print("## Per-key counts: the count pass before a gated query\n")
    print(f"Machine: {machine()}. Build: {a.label}, commit {commit()}. "
          f"Row groups, bytes and rows are exact; nothing here is timed.\n")

    # Ledger: 800,000 rows in 160 row groups over 200,000 entities.
    ledger = os.path.join(work, "ledger")
    shutil.rmtree(ledger, ignore_errors=True)
    run(a.bin, ["bench", "generate", "--out", ledger, "--seed", "7", "--files", "40",
                "--row-groups-per-file", "4", "--rows-per-row-group", "5000",
                "--entities", "200000", "--churn", "0.1", "--days", "10",
                "--payload-bytes", "64"])
    data = os.path.join(ledger, "data") if os.path.isdir(os.path.join(ledger, "data")) else ledger
    present = set()
    for batch in ds.dataset(data, format="parquet").to_batches(columns=["entity_id"]):
        present.update(batch.column(0).to_pylist())
    sets = key_sets(present, lambda i: 10_000_000 + i, rng)
    rows = corpus_rows(a.bin, "ledger", data, "entity_id", pa.uint64(), sets, work, [])
    table(f"Ledger corpus: {len(present):,} entities present, key `entity_id`", rows,
          live_bytes(data, None))

    # Market: the small rung, hive-partitioned by day.
    market = os.path.join(work, "market")
    shutil.rmtree(market, ignore_errors=True)
    run(a.bin, ["bench", "market", "--out", market, "--rung", "small"])
    mdata = os.path.join(market, "data")
    present = set()
    for batch in ds.dataset(mdata, format="parquet", partitioning="hive").to_batches(columns=["symbol"]):
        present.update(batch.column(0).to_pylist())
    sets = key_sets(present, lambda i: f"X{i:06d}", rng)
    rows = corpus_rows(a.bin, "market", mdata, "symbol", pa.string(), sets, work,
                       ["--partitioning", "hive"])
    table(f"Market corpus (small rung): {len(present):,} symbols present, key `symbol`; larger "
          f"key sets add absent symbols", rows, live_bytes(mdata, "hive"))


if __name__ == "__main__":
    main()
