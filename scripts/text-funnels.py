#!/usr/bin/env python3
"""Measured funnels for text keys (B5) on both benchmark shapes.

For each shape (the ledger: attribute blobs searched by substring; the market
corpus: documents keyed by symbol and date, filtered by token), writes a text
corpus with `bellwether bench text` (1-4 KB documents, planted terms at 1% and
0.01% of rows, exact ground truth), then:

  1. prices every query as one text run with `bellwether why` on the
     unindexed folder (no index; each text key priced on transient segments,
     with its distinct-token count and the R-text-1 lint);
  2. builds each text key with `bellwether index` and runs every query with
     `bellwether locate --contains / --has`, asserting that the plan holds
     every ground-truth row group (invariant 1) and that it is exactly what
     `why` predicted for that key;
  3. prints one markdown table per shape: row groups and bytes per query
     under no index, a words dictionary, and trigrams, and the ground-truth
     minimum (the row groups holding a match).

Drives the `bellwether` binary only: every number is a plan the CLI printed
with --json (exact sums of row-group statistics, never estimates) or the
corpus's ground truth. No timing is measured.

Usage: scripts/text-funnels.py --bin PATH/TO/bellwether [--work DIR]
       [--label "debug build"]
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


def js(bin_, args):
    return json.loads(run(bin_, ["--json", *args]))


def planned(v, base):
    out = set()
    for e in v["evidence"]["record"]["body"]["plan"]["entries"]:
        rel = os.path.relpath(e["file"]["path"], base)
        for rg in e["row_groups"]:
            out.add((rel, rg))
    return out


def machine():
    m = platform.machine()
    model = ""
    if sys.platform == "darwin":
        try:
            model = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                   capture_output=True, text=True).stdout.strip()
        except OSError:
            pass
    return f"{platform.system()} {m}{', ' + model if model else ''}"


def flag(op):
    return {"contains": "--contains", "has_token": "--has"}[op]


def size(n):
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def dir_bytes(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total


def shape_funnel(bin_, work, shape, label):
    out = os.path.join(work, shape)
    shutil.rmtree(out, ignore_errors=True)
    wrote = js(bin_, ["bench", "text", "--shape", shape, "--out", out])
    summary = wrote["text"]
    out = os.path.realpath(out)
    data = os.path.join(out, "data")
    queries = summary["queries"]
    # One run per query, recorded in flag order: every --contains, then
    # every --has (the CLI numbers runs text-1, text-2, ... in that order).
    ordered = [q for q in queries if q["op"] == "contains"] + \
              [q for q in queries if q["op"] == "has_token"]
    args = ["why", data, "--index", os.path.join(work, f"idx-{shape}-why")]
    for q in ordered:
        args += [flag(q["op"]), f"body={q['value']}"]
    why = js(bin_, args)
    run_of = {f"text-{i + 1}": q["id"] for i, q in enumerate(ordered)}
    rules = [l["rule"] for l in why["profile"]["lints"]]
    assert "R-text-1" in rules, rules
    current = {}
    predicted = {}
    tokens = {}
    for c in why["candidates"]:
        tok = c["spec"]["tokenizer"]
        tokens[tok] = (c["kind_column"]["value"], c["kind_column"]["label"])
        rec = c["replay"]["record"]
        for r in rec["current"]:
            current[run_of[r["run_id"]]] = r
        for r in rec["proposed"]:
            predicted[(tok, run_of[r["run_id"]])] = r
    # Build each key and run every query through it.
    realized = {}
    index_bytes = {}
    for tok in ["words", "trigram"]:
        idx = os.path.join(work, f"idx-{shape}-{tok}")
        shutil.rmtree(idx, ignore_errors=True)
        js(bin_, ["index", data, "--index", idx, "--key", f"text:body,tokenizer={tok}"])
        index_bytes[tok] = dir_bytes(os.path.join(idx, "segments"))
        for q in queries:
            if tok == "words" and q["op"] == "contains":
                continue
            v = js(bin_, ["locate", data, "--index", idx, flag(q["op"]), f"body={q['value']}"])
            got = planned(v, out)
            want = {(t["file"], t["row_group"]) for t in q["matching_row_groups"]}
            assert want <= got, (shape, tok, q["id"], want - got)
            p = predicted[(tok, q["id"])]
            assert (p["row_groups"], p["bytes"]) == (v["planned"]["row_groups"], v["planned"]["bytes"]), \
                (shape, tok, q["id"], p, v["planned"])
            realized[(tok, q["id"])] = v["planned"]
    live = summary["row_groups"]
    print(f"### {shape.capitalize()} shape\n")
    print(f"{summary['files']} files, {live} row groups, {summary['rows']:,} rows, "
          f"{size(summary['body_bytes'])} of text in `body` (1-4 KB per row). "
          f"Measured on {machine()}, {label}.\n")
    print("| query | match | share of rows | rows | no index | text:body,tokenizer=words | "
          "text:body,tokenizer=trigram | minimum (truth) |")
    print("|---|---|---:|---:|---|---|---|---:|")
    # Positive cases first: the queries a text key prunes, then the ones no
    # row-group index can prune (a term in every row group).
    def pruned(q):
        r = realized.get(("trigram", q["id"]))
        return 0 if r and r["row_groups"] < live else 1
    for q in sorted(queries, key=lambda q: (pruned(q), q["selectivity"], q["id"])):
        cur = current[q["id"]]
        cell = lambda r: f"{r['row_groups']} rg, {size(r['bytes'])}"
        words = realized.get(("words", q["id"]))
        block = " (one block)" if shape == "ledger" else " (one symbol, one day)"
        share = f"{q['selectivity'] * 100:.2g}%" + (block if q["clustered"] else "")
        print(f"| `{q['value']}` | {q['op']} | {share} | {q['matching_rows']} | {cell(cur)} | "
              f"{cell(words) if words else 'does not serve contains'} | "
              f"{cell(realized[('trigram', q['id'])])} | {len(q['matching_row_groups'])} rg |")
    print()
    for tok in ["words", "trigram"]:
        value, lab = tokens[tok]
        print(f"- `text:body,tokenizer={tok}`: {int(value):,} {lab} (the `why` text column); "
              f"segments {size(index_bytes[tok])}.")
    print(f"- R-text-1 fired on the unindexed folder; every plan held every ground-truth row group, "
          f"and every built key planned exactly the row groups and bytes `why` predicted.\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work")
    ap.add_argument("--label", default="debug build")
    a = ap.parse_args()
    work = a.work or tempfile.mkdtemp(prefix="bw-text-")
    print("## Text keys (B5): row groups and bytes per query\n")
    print("Exact sums of row-group statistics under each plan; `no index` reads every row group. "
          "A trigram plan is a candidate set (the residual is mandatory); a words plan holds only "
          "row groups with a match.\n")
    for shape in ["ledger", "market"]:
        shape_funnel(a.bin, work, shape, a.label)


if __name__ == "__main__":
    main()
