#!/usr/bin/env python3
"""Measured funnel for the `sketch` sidecar (B2) on both bench shapes.

The question both shapes ask is "is this column a key?" (`count(distinct
col)`), and the funnel counts the data files a `why` diagnosis has to read to
answer it:

  ledger  one day of the synthetic ledger in many small files: `batch_id` is a
          batch timestamp whose every value covers a large share of the rows,
          beside the true id `entity_id`.
  market  the time-series `market` corpus in many small files per day: the
          vendor-onboarding check that `(symbol, ts)` is unique per file.

For each shape it runs, through the `bellwether` binary only:

  1. `why` on the folder with no index: the profile reads every file's key
     columns (`reads.files`) to measure fanout and fire the batch-stamp lint;
  2. `index --key sketch:...`: the one pass that writes the sketch segments
     (`row_groups_read`);
  3. `why --sketches-only`: the same diagnosis from the sketch segments alone
     (`reads.files` = 0), with the low-cardinality-key lint `R-sketch-1`;
  4. `why` with the sketch present (the profile still reads; the `sketch`
     column is added beside it);
  5. `sketch cardinality`: every item's approximate distinct count and error
     bound, against an independent oracle (pyarrow over the decoded files),
     and on `market` the files the sketch says repeat a `(symbol, ts)` value
     against the files that do.

Every count of reads, rows, and files is exact; every distinct count from a
sketch is approximate and printed with its interval. No timing is measured.

Usage: scripts/sketch-funnels.py --bin PATH/TO/bellwether [--work DIR]
       [--shape ledger|market|both] [--ledger-files 10000]
       [--market-files-per-day 1000] [--label "machine, build"]
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections import Counter


def run(bin_, args):
    out = subprocess.run([bin_, *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"bellwether {' '.join(args)} failed ({out.returncode}): {out.stderr.strip()}")
    return out.stdout


def lookup(bin_, args):
    return json.loads(run(bin_, ["--json", *args]))


def rules(report, column):
    """Rule ids of the lints naming `column` first."""
    return sorted({l["rule"] for l in report["profile"]["lints"] if l["columns"][:1] == [column]})


def oracle_distinct(files, columns):
    """Exact distinct values of a column tuple (rows whose leading column is
    null left out, as the sketch does), and the files that repeat a value,
    from the decoded files (pyarrow), or None when pyarrow is missing."""
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
    except ImportError:
        return None
    tables = []
    dup_files = 0
    for f in files:
        t = pq.read_table(f, columns=columns, partitioning=None)
        t = t.filter(pc.is_valid(t.column(columns[0])))
        if t.group_by(columns).aggregate([]).num_rows < t.num_rows:
            dup_files += 1
        tables.append(t)
    whole = pa.concat_tables(tables)
    return {
        "distinct": whole.group_by(columns).aggregate([]).num_rows,
        "rows": whole.num_rows,
        "files_with_duplicates": dup_files,
    }


def data_files(root):
    out = []
    for d, _, fs in os.walk(root):
        if ".bellwether" in d.split(os.sep):
            continue
        out += [os.path.join(d, f) for f in fs if f.endswith(".parquet")]
    return sorted(out)


def funnel(bin_, name, data, part, sketch, batch_col, items, work):
    """Runs steps 1-5 on one corpus; returns the measured rows."""
    idx = os.path.join(work, f"idx-{name}")
    shutil.rmtree(idx, ignore_errors=True)
    shutil.rmtree(os.path.join(data, ".bellwether"), ignore_errors=True)
    files = data_files(data)

    plain = lookup(bin_, ["why", data, *part])
    built = lookup(bin_, ["index", data, *part, "--index", idx, "--key", sketch])
    only = lookup(bin_, ["why", data, *part, "--index", idx, "--sketches-only"])
    both = lookup(bin_, ["why", data, *part, "--index", idx])
    card = lookup(bin_, ["sketch", "cardinality", data, *part, "--index", idx])

    for r in (plain, only, both):
        assert r["reads"]["files"] <= len(files), r["reads"]
    assert only["reads"] == {"files": 0, "row_groups": 0}, only["reads"]
    assert only["from_sketches"] is True

    truth = {}
    for it in items:
        truth[it] = oracle_distinct(files, it.split("+"))

    return {
        "files": len(files),
        "plain": plain,
        "built": built,
        "only": only,
        "both": both,
        "card": card,
        "truth": truth,
        "batch_col": batch_col,
    }


def report(name, m, label):
    f = m["files"]
    b = m["batch_col"]
    print(f"### {name}: {f} files ({label})\n")
    print("| diagnosis | data files read | row groups read | lints on `" + b + "` | sketch column |")
    print("|---|---:|---:|---|---|")
    for title, r in [
        ("`why`, no index", m["plain"]),
        ("`why --sketches-only`", m["only"]),
        ("`why`, sketch present", m["both"]),
    ]:
        col = ", ".join(
            f"{c['cardinality']['item']} ≈{c['cardinality']['distinct']}" for c in r.get("sketch", [])
        ) or "—"
        print(f"| {title} | {r['reads']['files']} | {r['reads']['row_groups']} | "
              f"{', '.join(rules(r, b)) or 'none'} | {col} |")
    print()
    print(f"One-time cost of the sketch: `index --key sketch:...` read "
          f"{m['built']['row_groups_read']} row groups and wrote {m['built']['segments_written']} "
          f"segment(s); every later diagnosis reads none.\n")
    print("| item | rows | ≈distinct (sketch) | 99.7% interval | exact distinct (oracle) | inside | "
          "rows per value | files repeating a value: sketch / oracle |")
    print("|---|---:|---:|---|---:|---|---:|---|")
    for it in m["card"]["items"]:
        c = it["cardinality"]
        t = m["truth"].get(c["item"])
        if t is None:
            exact, inside, dup = "n/a", "n/a", f"{c['files_with_duplicates']} / n/a"
        else:
            exact = t["distinct"]
            inside = "yes" if c["distinct_low"] <= exact <= c["distinct_high"] else "**no**"
            dup = f"{c['files_with_duplicates']} / {t['files_with_duplicates']}"
        print(f"| `{c['item']}` | {c['rows']} | {c['distinct']} | "
              f"[{c['distinct_low']}, {c['distinct_high']}] (±{300 * c['relative_standard_error']:.1f}%) | "
              f"{exact} | {inside} | ≈{c['rows_per_value']:.0f} | {dup} |")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work")
    ap.add_argument("--shape", choices=["ledger", "market", "both"], default="both")
    ap.add_argument("--ledger-files", type=int, default=10_000)
    ap.add_argument("--market-files-per-day", type=int, default=1_000)
    ap.add_argument("--label", default="debug build")
    a = ap.parse_args()
    work = os.path.realpath(a.work or tempfile.mkdtemp(prefix="bw-sketch-"))
    os.makedirs(work, exist_ok=True)
    results = {}

    if a.shape in ("ledger", "both"):
        out = os.path.join(work, "ledger")
        shutil.rmtree(out, ignore_errors=True)
        # One day, one 1,000-row row group per file: 72 twenty-minute batch
        # windows share the day's rows, so each batch_id value covers about
        # rows / 72 of them, beside an entity id with far more values.
        run(a.bin, ["bench", "generate", "--out", out, "--files", str(a.ledger_files),
                    "--days", "1", "--row-groups-per-file", "1", "--rows-per-row-group", "1000",
                    "--entities", "1000000", "--churn", "1", "--payload-bytes", "16"])
        m = funnel(a.bin, "ledger", os.path.join(out, "data"), [],
                   "sketch:entity_id,batch_id", "batch_id", ["entity_id", "batch_id"], work)
        report("Ledger", m, a.label)
        results["ledger"] = m

    if a.shape in ("market", "both"):
        out = os.path.join(work, "market")
        shutil.rmtree(out, ignore_errors=True)
        run(a.bin, ["--json", "bench", "market", "--out", out, "--rung", "small",
                    "--files-per-day", str(a.market_files_per_day)])
        m = funnel(a.bin, "market", os.path.join(out, "data"), ["--partitioning", "hive"],
                   "sketch:symbol,ts,symbol+ts", "symbol", ["symbol", "ts", "symbol+ts"], work)
        report("Market", m, a.label)
        results["market"] = m

    with open(os.path.join(work, "sketch-funnels.json"), "w") as fh:
        json.dump(results, fh, indent=1)
    print(f"Raw results: {os.path.join(work, 'sketch-funnels.json')}")


if __name__ == "__main__":
    main()
