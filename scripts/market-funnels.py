#!/usr/bin/env python3
"""Measured funnels for the WP21 time-series capabilities on the `market` corpus.

Drives the `bellwether` binary only (every number is a plan the CLI printed
with --json, or the corpus's ground truth), and prints one markdown table per
capability:

  1. partition values from paths: a day window on the `day=` directory column
  2. ordered plans: how many window plans hold a total time order
  3. pinned lookups: plans at an earlier manifest after the folder grew
  4. per-key time bounds: symbol-set x window, keys only / + range key /
     + per-key bounds / ground-truth minimum
  5. bitemporal as-of: planned row groups against the ground-truth winners,
     and the valid-time-only baseline's disagreements

Usage: scripts/market-funnels.py --bin PATH/TO/bellwether [--rung small|medium]
       [--work DIR] [--label "debug build"]

Row groups and bytes are exact sums of statistics (plans), never estimates.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict


def run(bin_, args):
    out = subprocess.run([bin_, *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"bellwether {' '.join(args)} failed ({out.returncode}): {out.stderr.strip()}")
    return out.stdout


def lookup(bin_, args):
    return json.loads(run(bin_, [*args, "--json"]))


def planned(v, base):
    out = set()
    for e in v["evidence"]["record"]["body"]["plan"]["entries"]:
        rel = os.path.relpath(e["file"]["path"], base)
        for rg in e["row_groups"]:
            out.add((rel, rg))
    return out


def rgs(lst):
    return {(x["file"], x["row_group"]) for x in lst}


def index(bin_, data, where, keys):
    args = ["index", data, "--partitioning", "hive", "--index", where]
    for k in keys:
        args += ["--key", k]
    return lookup(bin_, args)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--rung", default="small")
    ap.add_argument("--work")
    ap.add_argument("--label", default="debug build")
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="bw-market-")
    out = os.path.join(work, "market")
    shutil.rmtree(out, ignore_errors=True)
    summary = lookup(a.bin, ["bench", "market", "--out", out, "--rung", a.rung])
    out = os.path.realpath(out)
    data = os.path.join(out, "data")
    workload = json.load(open(os.path.join(out, "workload", "market.json")))
    truth = {q["id"]: q for q in json.load(open(os.path.join(out, "workload", "truth.json")))["queries"]}
    queries = workload["queries"]
    idx = lambda name: os.path.join(work, "idx-" + name)
    for name in ["keys", "range", "bounds", "day", "t2", "t1"]:
        shutil.rmtree(idx(name), ignore_errors=True)
    index(a.bin, data, idx("keys"), ["exact:symbol"])
    index(a.bin, data, idx("range"), ["exact:symbol", "range:ts"])
    index(a.bin, data, idx("bounds"), ["exact:symbol@ts", "range:ts"])
    index(a.bin, data, idx("day"), ["exact:symbol", "range:day"])
    index(a.bin, data, idx("t2"), ["temporal2:symbol@ts,known_at"])
    index(a.bin, data, idx("t1"), ["temporal:symbol@ts"])
    m = summary.get("market") or {}
    print(f"## Market corpus ({a.rung} rung, {a.label})\n")
    print(f"{m.get('files')} files, {m.get('row_groups')} row groups, {m.get('rows')} rows, "
          f"{m.get('restatements')} restatements, {m.get('late_prints')} late prints, "
          f"{m.get('bytes')} bytes; {len(queries)} queries.\n")

    # 4 and 2: windows.
    by_span = defaultdict(lambda: defaultdict(int))
    ordered = 0
    windows = [q for q in queries if q["kind"] == "window"]
    for q in windows:
        keys = os.path.join(out, q["keys_file"])
        t = truth[q["id"]]["window"]
        span = q["window_span"]
        row = by_span[span]
        row["queries"] += 1
        base = ["locate", data, "--keys", keys]
        win = ["--from", str(q["from_us"]), "--to", str(q["to_us"]), "--ts", "ts"]
        k = lookup(a.bin, [*base, "--index", idx("keys")])
        r = lookup(a.bin, [*base, *win, "--index", idx("range")])
        b = lookup(a.bin, [*base, *win, "--index", idx("bounds")])
        got = planned(b, out)
        assert rgs(t["matching_row_groups"]) <= got, q["id"]
        row["total"] += k["row_groups_total"]
        row["keys"] += k["planned"]["row_groups"]
        row["keys_bytes"] += k["planned"]["bytes"]
        row["range"] += r["planned"]["row_groups"]
        row["range_bytes"] += r["planned"]["bytes"]
        row["bounds"] += b["planned"]["row_groups"]
        row["bounds_bytes"] += b["planned"]["bytes"]
        row["minimum"] += len(t["matching_row_groups"])
        ordered += b["ordered_by"] == "ts"
    print("### 4. Per-key time bounds: symbol set x window (row groups, summed over queries)\n")
    print("| window | queries | live | exact:symbol | + range:ts | exact:symbol@ts | minimum (truth) | bytes: keys / + range / + bounds |")
    print("|---|---:|---:|---:|---:|---:|---:|---|")
    for span, r in sorted(by_span.items()):
        print(f"| {span} | {r['queries']} | {r['total']} | {r['keys']} | {r['range']} | {r['bounds']} | "
              f"{r['minimum']} | {r['keys_bytes']} / {r['range_bytes']} / {r['bounds_bytes']} |")
    print()
    print("### 2. Ordered plans\n")
    print(f"{ordered} of {len(windows)} window plans hold a total order by `ts` row-group minimum "
          f"(the rest say why not: files that overlap in time, or a file's row groups out of time order).\n")

    # 1: partition days.
    days = sorted({q["from"][:10] for q in windows})
    tot = sel = 0
    for d in days:
        keys = os.path.join(out, windows[0]["keys_file"])
        v = lookup(a.bin, ["locate", data, "--keys", keys, "--ts", "day", "--from", d, "--to", d,
                           "--index", idx("day")])
        assert all(f.startswith(f"data/day={d}/") for f, _ in planned(v, out))
        tot += v["row_groups_total"]
        sel += v["planned"]["row_groups"]
    print("### 1. Partition values from paths\n")
    print(f"{len(days)} one-day windows on the `day=` column (one symbol): {sel} of {tot} live row groups "
          f"planned, all under the asked day's directory; the day column is never read from a data page.\n")

    # 5: bitemporal.
    exact = n = t2_rgs = t1_wrong = live = 0
    for q in queries:
        if q["kind"] not in ("asof", "latest_known"):
            continue
        keys = os.path.join(out, q["keys_file"])
        want = set()
        for w in truth[q["id"]]["winners"]:
            want |= rgs(w["row_groups"])
        if q["kind"] == "asof":
            args = ["asof", data, "--keys", keys, "--valid", str(q["valid_us"]), "--known", str(q["known_us"])]
            base = ["asof", data, "--keys", keys, "--at", str(q["valid_us"])]
        else:
            args = ["latest", data, "--keys", keys, "--known", str(q["known_us"])]
            base = ["latest", data, "--keys", keys]
        v = lookup(a.bin, [*args, "--index", idx("t2")])
        u = lookup(a.bin, [*base, "--index", idx("t1")])
        n += 1
        live += v["row_groups_total"]
        t2_rgs += v["planned"]["row_groups"]
        exact += planned(v, out) == want
        t1_wrong += planned(u, out) != want
    print("### 5. Bitemporal as-of\n")
    print(f"{n} as-of / latest-known queries: temporal2 planned {t2_rgs} of {live} live row groups, "
          f"exactly the ground-truth winners' row groups in {exact} of {n}; a valid-time-only temporal key "
          f"plans different row groups in {t1_wrong} of {n} (it cannot see when a value became known).\n")

    # 3: pinned.
    grow = os.path.join(work, "grow")
    shutil.rmtree(grow, ignore_errors=True)
    shutil.copytree(data, grow, ignore=shutil.ignore_patterns(".bellwether"))
    last = sorted(d for d in os.listdir(grow) if d.startswith("day="))[-1]
    held = os.path.join(work, "held-" + last)
    shutil.rmtree(held, ignore_errors=True)
    shutil.move(os.path.join(grow, last), held)
    gidx = idx("grow")
    shutil.rmtree(gidx, ignore_errors=True)
    s1 = index(a.bin, grow, gidx, ["exact:symbol@ts", "range:ts"])["sequence"]
    before = {}
    for q in windows:
        keys = os.path.join(out, q["keys_file"])
        v = lookup(a.bin, ["locate", grow, "--keys", keys, "--from", str(q["from_us"]), "--to",
                           str(q["to_us"]), "--index", gidx])
        before[q["id"]] = (v["evidence"]["record"]["body"]["plan"], v["row_groups_total"])
    shutil.move(held, os.path.join(grow, last))
    s2 = index(a.bin, grow, gidx, [])["sequence"]
    same = 0
    totals = (0, 0)
    for q in windows:
        keys = os.path.join(out, q["keys_file"])
        v = lookup(a.bin, ["locate", grow, "--keys", keys, "--from", str(q["from_us"]), "--to",
                           str(q["to_us"]), "--index", gidx, "--at-manifest", str(s1)])
        plan = v["evidence"]["record"]["body"]["plan"]
        same += json.dumps(plan, sort_keys=False) == json.dumps(before[q["id"]][0], sort_keys=False)
        cur = lookup(a.bin, ["locate", grow, "--keys", keys, "--from", str(q["from_us"]), "--to",
                             str(q["to_us"]), "--index", gidx])
        totals = (totals[0] + before[q["id"]][1], totals[1] + cur["row_groups_total"])
    print("### 3. Pinned lookups\n")
    print(f"Indexed without the last day ({last}) at sequence {s1}, then with it at sequence {s2}: "
          f"{same} of {len(windows)} window plans at `--at-manifest {s1}` equal the plans recorded at "
          f"sequence {s1} byte for byte (live row groups per lookup: {totals[0] // len(windows)} then "
          f"{totals[1] // len(windows)}).\n")


if __name__ == "__main__":
    main()
