#!/usr/bin/env python3
"""Measured funnels for join keys (B3) on both bench shapes.

Drives the `bellwether` binary only: every number is a plan or a price the
CLI printed with --json, or the corpus's ground truth (`bench joins` writes
it while generating the rows, never from an index). For each shape (ledger
orders and payables on order_id; market trades and quotes on (symbol,
minute)), each with 1:N skew, and each recorded semi-join run (10, 10^3,
10^5 keys, bracketing an `in` list gate of 1,000):

  dataset          every live row group of both datasets and their bytes
  no join key      what `why --mode semijoin` prices the run at before the
                   join key exists (its `current`): each step locates every
                   asked key by the exact key on its side
  join key         `join semijoin` through the join key: each step reads the
                   row groups holding a key both datasets hold
  truth            row groups holding a key both datasets hold (the minimum)

and checks, per run and per side, that the join key's plan holds every truth
row group (invariant 1 per side), and that `why` priced the join key, before
it existed, at exactly what `join semijoin` then planned. It also records
the three runs as one workload (`why --from` repeated) and reports whether
R-join-1 fired before the join key and stays silent after it.

Usage: scripts/join-funnels.py --bin PATH/TO/bellwether [--work DIR]
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

GATE = 1000


def run(bin_, args):
    out = subprocess.run([bin_, *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"bellwether {' '.join(args)} failed ({out.returncode}): {out.stderr.strip()}")
    return out.stdout


def lookup(bin_, args):
    return json.loads(run(bin_, [*args, "--json"]))


def planned(evidence, data):
    out = set()
    for e in evidence["record"]["body"]["plan"]["entries"]:
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


def rules(w):
    return [l["rule"] for l in w["profile"]["lints"]]


def shape_funnel(a, shape, work):
    out = os.path.join(work, shape)
    shutil.rmtree(out, ignore_errors=True)
    gen = ["bench", "joins", "--shape", shape, "--out", out]
    if a.small:
        gen += ["--size", "40000" if shape == "ledger" else "400", "--rows-per-row-group", "2000",
                "--keysets", "10,1000,10000"]
    summary = lookup(a.bin, gen)["joins"]
    out = os.path.realpath(out)
    workload = json.load(open(os.path.join(out, "workload", "joins.json")))
    truth = {t["id"]: t for t in json.load(open(os.path.join(out, "workload", "truth.json")))["runs"]}
    left, right = (os.path.join(out, workload["left"]), os.path.join(out, workload["right"]))
    cols = ",".join(workload["columns"])
    side = f"{workload['right']}.{cols}"
    run(a.bin, ["index", right, "--key", f"exact:{cols}"])
    run(a.bin, ["index", left, "--key", f"exact:{cols}"])
    files = [os.path.join(out, r["file"]) for r in workload["runs"]]

    # Price every run before the join key exists, and all three as one
    # workload (R-join-1).
    priced = {}
    for r, f in zip(workload["runs"], files):
        w = lookup(a.bin, ["why", left, "--from", f, "--mode", "semijoin", "--join", side])
        floor = w["candidates"][0]
        assert floor["spec"]["kind"] == "join", floor["spec"]
        priced[r["id"]] = (w["current"], floor["proposed"], floor["kind_column"])
    many = [x for f in files for x in ("--from", f)]
    before = lookup(a.bin, ["why", left, *many, "--mode", "semijoin", "--join", side])
    built = lookup(a.bin, ["index", left, "--key", workload["spec"]])
    after = lookup(a.bin, ["why", left, *many, "--mode", "semijoin", "--join", side])
    overlap = lookup(a.bin, ["join", "overlap", left, "--join", side])

    rows = []
    for r, f in zip(workload["runs"], files):
        s = lookup(a.bin, ["join", "semijoin", left, "--join", side, "--keys", f])
        t = truth[r["id"]]
        for name, step, data in [("left", s["left"], left), ("right", s["right"], right)]:
            got = planned(step["evidence"], data)
            want = {(g[0], g[1]) for g in t[name]["row_groups"]}
            assert want <= got, f"{shape} {r['id']} {name}: invariant 1 ({len(want - got)} missed)"
        assert s["matched"] == t["matched"], (s["matched"], t["matched"])
        current, predicted, kind_column = priced[r["id"]]
        plan_rg = s["left"]["planned"]["row_groups"] + s["right"]["planned"]["row_groups"]
        plan_b = s["left"]["planned"]["bytes"] + s["right"]["planned"]["bytes"]
        same = (predicted["row_groups"], predicted["bytes"]) == (plan_rg, plan_b)
        assert same, f"{shape} {r['id']}: why predicted {predicted}, semijoin planned {plan_rg}/{plan_b}"
        rows.append((r, s, t, current, plan_rg, plan_b, kind_column))

    ls, rs = summary["left"], summary["right"]
    print(f"### {shape}: `{workload['left']}` ⋉ `{workload['right']}` on ({cols})\n")
    print(f"{workload['left']}: {ls['files']} files, {ls['row_groups']} row groups, {ls['rows']:,} rows, "
          f"{human(ls['bytes'])}, {ls['keys']:,} keys. {workload['right']}: {rs['files']} files, "
          f"{rs['row_groups']} row groups, {rs['rows']:,} rows, {human(rs['bytes'])}, {rs['keys']:,} keys. "
          f"{summary['both']:,} keys on both; right rows per shared key p50 / p95 / max "
          f"{summary['right_rows_per_key'][0]} / {summary['right_rows_per_key'][1]} / "
          f"{summary['right_rows_per_key'][2]} (1:N). `join overlap`: |L∩R| {overlap['both']:,}, "
          f"|L∖R| {overlap['left_only']:,}, |R∖L| {overlap['right_only']:,}, right row groups per shared key "
          f"max {overlap['max_right_row_groups']}. `index --key {workload['spec']}` committed sequence "
          f"{built['sequence']}.\n")
    print("| run | keys | on both | in:1,000 gate | dataset (row groups / bytes) | no join key: two locates "
          "(why current) | join key (join semijoin) | truth (row groups / rows, L + R) | why predicted = planned |")
    print("|---|---:|---:|:-:|---|---|---|---|:-:|")
    total_rg = ls["row_groups"] + rs["row_groups"]
    total_b = ls["bytes"] + rs["bytes"]
    for r, s, t, current, plan_rg, plan_b, kind_column in rows:
        truth_rg = len(t["left"]["row_groups"]) + len(t["right"]["row_groups"])
        gate = "within" if s["matched"] <= GATE else "over"
        print(f"| {r['id']} | {r['keys']:,} | {s['matched']:,} | {gate} | {total_rg} / {human(total_b)} "
              f"| {current['row_groups']} / {human(current['bytes'])} "
              f"| {plan_rg} / {human(plan_b)} ({100 * plan_b / max(current['bytes'], 1):.1f}%) "
              f"| {truth_rg} / {t['left']['rows'] + t['right']['rows']:,} | yes |")
    fired = "R-join-1" in rules(before)
    silent = "R-join-1" not in rules(after)
    print(f"\nThe three runs as one workload: R-join-1 {'fires' if fired else 'does not fire'} before the join "
          f"key, and is {'silent' if silent else 'STILL RAISED'} after it.\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work")
    ap.add_argument("--label", default="debug build")
    ap.add_argument("--small", action="store_true", help="a quick corpus, not the preset")
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="bw-joins-")
    rev = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                         text=True).stdout.strip()
    print("## Join keys (B3): semi-join funnels\n")
    print(f"Machine: {machine()}. Build: {a.label}, commit {rev}. Row groups and bytes are exact sums "
          f"of statistics; no timing is reported.\n")
    for shape in ["ledger", "market"]:
        shape_funnel(a, shape, work)


if __name__ == "__main__":
    main()
