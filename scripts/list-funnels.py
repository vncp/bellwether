#!/usr/bin/env python3
"""Measured funnels for list keys (B4) on both bench shapes.

Drives the `bellwether` binary only: every number is a plan or a price the
CLI printed with --json, or the corpus's ground truth (`bench lists` writes
it while generating the rows, never from an index). For each shape (ledger
attributes, market holdings) and each recorded membership run
(`contains_any` at 10, 10^3, 10^5 terms; one `contains_all`):

  dataset          every live row group and its bytes
  no list key      what `why` prices the run at before the key exists
                   (its `current`): every row group, since nothing prunes
                   by membership. The Parquet-statistics baseline reads the
                   same for a membership run (a list's statistics are on its
                   nested leaf); `crates/bellwether-replay/tests/list.rs`
                   asserts that, this script does not measure it.
  list key         `locate --any/--all` through `list:<column>`
  truth            row groups holding a matching row (the minimum)

and checks, per run, that the list key's plan holds every truth row group
(invariant 1) and that `why --mode contains-any|contains-all` priced the
list key, before it existed, at exactly what `locate` then planned.

Usage: scripts/list-funnels.py --bin PATH/TO/bellwether [--work DIR]
       [--label "debug build"] [--small]

Row groups and bytes are exact sums of statistics, never estimates.
"""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile


def run(bin_, args):
    out = subprocess.run([bin_, *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"bellwether {' '.join(args)} failed ({out.returncode}): {out.stderr.strip()}")
    return out.stdout


def lookup(bin_, args):
    return json.loads(run(bin_, [*args, "--json"]))


def planned(v, data):
    out = set()
    for e in v["evidence"]["record"]["body"]["plan"]["entries"]:
        rel = os.path.relpath(e["file"]["path"], data)
        for rg in e["row_groups"]:
            out.add((rel, rg))
    return out


def machine():
    cpu = platform.processor() or platform.machine()
    try:
        cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True,
                             text=True, check=True).stdout.strip() or cpu
    except (OSError, subprocess.CalledProcessError):
        pass
    return f"{cpu}, {platform.system()} {platform.release()}"


def human(n):
    for unit in ["B", "KiB", "MiB", "GiB"]:
        if n < 1024 or unit == "GiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024


def shape_funnel(a, shape, work):
    out = os.path.join(work, shape)
    shutil.rmtree(out, ignore_errors=True)
    gen = ["bench", "lists", "--shape", shape, "--out", out]
    if a.small:
        gen += ["--files", "8", "--rows-per-row-group", "1000", "--vocabulary", "20000",
                "--terms", "10,1000,10000"]
    summary = lookup(a.bin, gen)["lists"]
    out = os.path.realpath(out)
    data = os.path.join(out, "data")
    workload = json.load(open(os.path.join(out, "workload", "lists.json")))
    truth = {t["id"]: t for t in json.load(open(os.path.join(out, "workload", "truth.json")))["runs"]}
    column = workload["column"]
    part = ["--partitioning", "hive"] if shape == "market" else []
    idx = ["--index", os.path.join(work, f"idx-{shape}")]
    shutil.rmtree(idx[1], ignore_errors=True)

    # Price every run before the list key exists.
    priced = {}
    for r in workload["runs"]:
        mode = "contains-all" if r["query_kind"] == "contains_all" else "contains-any"
        w = lookup(a.bin, ["why", data, *part, *idx, "--from", os.path.join(out, r["file"]),
                           "--mode", mode])
        floor = w["candidates"][0]
        assert floor["spec"] == {"kind": "list", "column": column}, floor["spec"]
        priced[r["id"]] = (w["current"], floor["proposed"], floor["kind_column"], w)

    built = lookup(a.bin, ["index", data, *part, *idx, "--key", f"list:{column}"])

    rows = []
    for r in workload["runs"]:
        flag = "--all" if r["query_kind"] == "contains_all" else "--any"
        v = lookup(a.bin, ["locate", data, *part, *idx, "--keys", os.path.join(out, r["file"]), flag])
        got = planned(v, data)
        want = {(g["file"], g["row_group"]) for g in truth[r["id"]]["row_groups"]}
        assert want <= got, f"{shape} {r['id']}: invariant 1 ({len(want - got)} truth row groups missed)"
        current, predicted, kind_column, w = priced[r["id"]]
        same = (predicted["row_groups"], predicted["bytes"]) == (v["planned"]["row_groups"],
                                                                 v["planned"]["bytes"])
        assert same, f"{shape} {r['id']}: why predicted {predicted}, locate planned {v['planned']}"
        rows.append((r, v, current, predicted, kind_column, len(want), truth[r["id"]]["rows"],
                     [l["rule"] for l in w["profile"]["lints"]]))

    print(f"### {shape}: `{column}` ({summary['files']} files, {summary['row_groups']} row groups, "
          f"{summary['rows']:,} rows, {human(summary['bytes'])})\n")
    print(f"{summary['distinct_elements']:,} distinct elements; row groups per element p50 / p95 / max "
          f"{summary['row_groups_per_element'][0]} / {summary['row_groups_per_element'][1]} / "
          f"{summary['row_groups_per_element'][2]} (a skewed head in every row group, a long tail in a "
          f"few); {summary['null_lists']:,} null and {summary['empty_lists']:,} empty lists (not indexed). "
          f"`index --key list:{column}` committed sequence {built['sequence']}.\n")
    print("| run | terms | dataset (row groups / bytes) | no list key (why current) | list key | truth (row groups / rows) "
          "| why predicted = planned | `list` column | R-list-1 before |")
    print("|---|---:|---|---|---|---|:-:|---:|:-:|")
    for r, v, current, predicted, kind_column, want, matching, lints in rows:
        p = v["planned"]
        total = v["row_groups_total"]
        print(f"| {r['id']} ({r['query_kind']}) | {r['terms']:,} | {total} / {human(current['bytes'])} "
              f"| {current['row_groups']} / {human(current['bytes'])} "
              f"| {p['row_groups']} / {human(p['bytes'])} ({100 * p['bytes'] / max(current['bytes'], 1):.1f}%) "
              f"| {want} / {matching:,} | yes | {kind_column['value']} distinct "
              f"| {'fires' if 'R-list-1' in lints else 'no'} |")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work")
    ap.add_argument("--label", default="debug build")
    ap.add_argument("--small", action="store_true", help="a quick corpus, not the preset")
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="bw-lists-")
    rev = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                         text=True).stdout.strip()
    print(f"## List keys (B4): membership funnels\n")
    print(f"Machine: {machine()}. Build: {a.label}, commit {rev}. Row groups and bytes are exact sums "
          f"of statistics; no timing is reported.\n")
    for shape in ["ledger", "market"]:
        shape_funnel(a, shape, work)


if __name__ == "__main__":
    main()
