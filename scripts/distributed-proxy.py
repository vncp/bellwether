#!/usr/bin/env python3
"""Same-host process proxy for the multi-machine build (WP22b, ADR 030).

Builds the `market` 10x corpus's index (the corpus and specs of
scripts/perf.py's build section) two ways on one machine, each into a fresh
index, `--reps` times:

  single  `bellwether index` with every thread;
  proxy   `index --plan-work --items N`, then N `index --worker` processes at
          once (each with every thread), then `index --commit`.

and checks that both commit the same manifest (apart from the fields every
commit stamps from the clock or a random source). This is a proxy: the
processes share one machine's cores, memory, page cache, and disk, so it
measures the protocol's overhead and contention, not multi-host speedup.
Label it so wherever it is published.

    cargo build --release -p bellwether-cli   # in a private target dir
    scripts/distributed-proxy.py --bin <target>/release/bellwether --work /scratch/dist

Prints a Markdown table and writes <work>/distributed-proxy.json.
"""

import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time

SPECS = ["exact:symbol@ts", "range:ts", "temporal:symbol@ts"]


def sh(args, **kw):
    r = subprocess.run(args, capture_output=True, text=True, **kw)
    if r.returncode != 0:
        sys.exit(f"{' '.join(args)} failed ({r.returncode}):\n{r.stdout}\n{r.stderr}")
    return r.stdout


def manifest(idx):
    with open(os.path.join(idx, "manifest.json")) as f:
        m = json.load(f)
    for k in ("committed_at", "commit_nonce", "created_at"):
        m.pop(k, None)
    for t in m.get("segment_tombstones", []):
        t.pop("since", None)
    return m


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--items", type=int, default=4)
    ap.add_argument("--reps", type=int, default=3)
    a = ap.parse_args()
    os.makedirs(a.work, exist_ok=True)
    corpus = os.path.join(a.work, "market-10x")
    if not os.path.isdir(os.path.join(corpus, "data")):
        sh([a.bin, "bench", "market", "--out", corpus, "--symbols", "3000", "--days", "30",
            "--prints-per-symbol-per-day", "12"])
    data = os.path.realpath(os.path.join(corpus, "data"))
    keys = [x for s in SPECS for x in ("--key", s)]
    rows = []
    for rep in range(a.reps):
        single = os.path.join(a.work, "idx-single")
        proxy = os.path.join(a.work, "idx-proxy")
        plan_dir = os.path.join(a.work, "plan")
        for d in (single, proxy, plan_dir):
            shutil.rmtree(d, ignore_errors=True)

        t0 = time.monotonic()
        sh([a.bin, "--quiet", "index", data, "--index", single, "--partitioning", "hive", *keys])
        t_single = time.monotonic() - t0

        t0 = time.monotonic()
        sh([a.bin, "--quiet", "index", data, "--index", proxy, "--partitioning", "hive", *keys,
            "--plan-work", plan_dir, "--items", str(a.items)])
        t_plan = time.monotonic() - t0
        plan = os.path.join(plan_dir, "plan.json")
        t1 = time.monotonic()
        procs = [subprocess.Popen([a.bin, "--json", "index", "--worker", plan, "--item", str(i)],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                 for i in range(a.items)]
        worker_ms = []
        for p in procs:
            out, err = p.communicate()
            if p.returncode != 0:
                sys.exit(f"worker failed: {err}")
            worker_ms.append(json.loads(out)["elapsed_ms"])
        t_workers = time.monotonic() - t1
        t2 = time.monotonic()
        sh([a.bin, "--quiet", "index", "--commit", plan])
        t_commit = time.monotonic() - t2
        t_proxy = time.monotonic() - t0
        same = manifest(single) == manifest(proxy)
        if not same:
            sys.exit("the proxy committed another manifest than the single-machine build")
        rows.append({"rep": rep, "single_s": t_single, "plan_s": t_plan, "workers_s": t_workers,
                     "worker_ms": worker_ms, "commit_s": t_commit, "proxy_s": t_proxy,
                     "same_manifest": same})

    def med(k):
        return statistics.median(r[k] for r in rows)

    out = {
        "what": "same-host process proxy of the multi-machine build (not multi-host)",
        "machine": f"{platform.machine()} {platform.system()} {platform.release()}, "
                   f"{os.cpu_count()} logical CPUs",
        "commit": subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                                 text=True).stdout.strip(),
        "build": "release",
        "corpus": "bellwether bench market --symbols 3000 --days 30 --prints-per-symbol-per-day 12",
        "specs": SPECS,
        "items": a.items,
        "reps": rows,
    }
    with open(os.path.join(a.work, "distributed-proxy.json"), "w") as f:
        json.dump(out, f, indent=2)
    print(f"{out['what']}; {out['machine']}; commit {out['commit']}; release; median of {a.reps}\n")
    print("| run | wall (s) |")
    print("|---|---:|")
    print(f"| 1 process, all threads (`index`) | {med('single_s'):.2f} |")
    print(f"| {a.items} processes, all threads each: plan | {med('plan_s'):.2f} |")
    print(f"| {a.items} processes: workers (all at once) | {med('workers_s'):.2f} |")
    print(f"| {a.items} processes: commit (checks every segment) | {med('commit_s'):.2f} |")
    print(f"| {a.items} processes: total | {med('proxy_s'):.2f} |")
    print("\nmanifests equal in every rep:", all(r["same_manifest"] for r in rows))


if __name__ == "__main__":
    main()
