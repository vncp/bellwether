#!/usr/bin/env python3
"""Measured funnels for `expr` keys on both benchmark shapes.

For each shape (`ledger`: a join key inside a JSON attribute column;
`market`: a vendor JSON payload keyed by symbol, plus `date_trunc(ts,day)`)
this writes the corpus with `bellwether bench expr`, then for each query
(equality on a JSON path at three selectivities):

  - `why --from <keys>` on the unindexed folder: what the query reads with
    no index (current) and what the `expr` key the engine proposes would
    read (predicted by replay), with the key's `expr` column;
  - `index --key expr:...:exact` and `...:bloom@0.01`, then `locate --keys`:
    what each key really plans;
  - the ground truth: the row groups holding a key, recorded by the
    generator from the rows it wrote.

It asserts that every plan holds every ground-truth row group (no false
negatives) and that the exact key's planned row groups and bytes equal the
replay's prediction, then prints one markdown table per shape, positive
cases first and the wide (no improvement) case last.

Usage: scripts/expr-funnels.py --bin PATH/TO/bellwether [--work DIR]
       [--label "debug build"]

Row groups and bytes are exact sums of statistics (plans), never estimates.
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


def shape_funnel(a, work, shape):
    out = os.path.join(work, shape)
    shutil.rmtree(out, ignore_errors=True)
    summary = lookup(a.bin, ["bench", "expr", "--shape", shape, "--out", out])["expr"]
    out = os.path.realpath(out)
    data = os.path.join(out, "data")
    truth = json.load(open(os.path.join(out, "workload", "truth.json")))
    names = {}
    for q in truth["queries"]:
        names.setdefault(q["expression"], "day" if q["id"] == "day" else ("account" if shape == "ledger" else "symbol"))
    exact = [f"expr:{n}={e}:exact" for e, n in names.items()]
    bloom = [f"expr:{n}={e}:bloom@0.01" for e, n in names.items()]
    idx = {}
    for kind, specs in [("exact", exact), ("bloom", bloom)]:
        idx[kind] = os.path.join(work, f"idx-{shape}-{kind}")
        shutil.rmtree(idx[kind], ignore_errors=True)
        args = ["index", data, "--index", idx[kind]]
        for s in specs:
            args += ["--key", s]
        lookup(a.bin, args)
    rows = []
    for q in truth["queries"]:
        keys = os.path.join(out, q["keys_file"])
        want = {(f, rg) for f, rg in q["row_groups"]}
        why_idx = os.path.join(work, f"why-{shape}-{q['id']}")
        shutil.rmtree(why_idx, ignore_errors=True)
        why = lookup(a.bin, ["why", data, "--from", keys, "--index", why_idx])
        cands = [c for c in why["candidates"] if c["spec"]["kind"] == "expr"]
        assert len(cands) == 1, f"{shape}/{q['id']}: expected one expr candidate, got {why['candidates']}"
        cand = cands[0]
        got = {}
        for kind in ["exact", "bloom"]:
            v = lookup(a.bin, ["locate", data, "--keys", keys, "--index", idx[kind]])
            p = planned(v, data)
            missing = want - p
            assert not missing, f"{shape}/{q['id']} {kind}: plan misses truth row groups {sorted(missing)[:5]}"
            got[kind] = v["planned"]
        assert (got["exact"]["row_groups"], got["exact"]["bytes"]) == (
            cand["proposed"]["row_groups"], cand["proposed"]["bytes"]
        ), f"{shape}/{q['id']}: planned {got['exact']} != predicted {cand['proposed']}"
        rows.append((q, why, cand, got, len(want)))
    order = {"rare": 0, "medium": 1, "day": 2, "wide": 3}
    rows.sort(key=lambda r: order.get(r[0]["id"], 9))
    print(f"### {shape}\n")
    print(f"{summary['files']} files, {summary['row_groups']} row groups, {summary['rows']:,} rows "
          f"({summary['null_rows']:,} without the keyed member), {summary['bytes']:,} bytes.\n")
    print("| query | expression | keys | no index: row groups / bytes | expr key, exact (predicted = planned) | "
          "expr key, bloom@0.01 | minimum (truth) | bytes saved | expr column |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---|")
    for q, why, cand, got, minimum in rows:
        cur = why["current"]
        saved = 1 - got["exact"]["bytes"] / cur["bytes"] if cur["bytes"] else 0
        col = (cand.get("kind_column") or {}).get("value", "")
        print(f"| {q['id']} | `{q['expression']}` | {len(q['keys'])} | {cur['row_groups']:,} / {cur['bytes']:,} | "
              f"{got['exact']['row_groups']:,} / {got['exact']['bytes']:,} | "
              f"{got['bloom']['row_groups']:,} / {got['bloom']['bytes']:,} | {minimum:,} | {saved:.0%} | {col} |")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work")
    ap.add_argument("--label", default="debug build")
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="bw-expr-")
    version = run(a.bin, ["--version"]).strip()
    print(f"## expr keys: equality on a JSON path at three selectivities\n")
    print(f"Machine: {platform.system()} {platform.machine()}, {os.cpu_count()} CPUs. "
          f"Build: {version}, {a.label}. Row groups and bytes are exact plan sums and do not "
          f"depend on the machine or build.\n")
    for shape in ["ledger", "market"]:
        shape_funnel(a, work, shape)


if __name__ == "__main__":
    main()
