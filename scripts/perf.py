#!/usr/bin/env python3
"""Release measurements for WP22 (docs/measured/*-performance.md).

Run through scripts/perf.sh, which builds the release binaries and passes
--bin. Every bellwether call is a real process; every number is either a
wall time this script measured, a time the binary reported about itself
(`bench read`, the latency tests, DuckDB's `.timer`), or an exact count the
binary printed. Writes:

  <out>/perf.json        format bellwether-perf/1 (the budget check reads it)
  <out>/samples.jsonl    every raw sample, one JSON object per line
  <out>/profiles/        samply profiles and their top self-time frames
  --showcase FILE        the timing rows for the README charts

Sections (--sections, comma-separated; default all):
  lookups    plan and end-to-end p50/p95 against a full scan and DuckDB
  build      row groups per second at --threads 1, 2, 4, 8, all
  reconcile  rows per second per side at four memory budgets, passes, peaks
  latency    replay p50 run, observe and MCP latency, resident bytes per key
             (contract v3 entry 32): release test builds, run under cargo
  why        `why` end to end on the small rung's pre-registered workload,
             cold and warm (the CLI only, so it runs without a target dir)
  profiles   samply over the five hot paths and the ledger build, top ten self-time frames
  ledger     `ledger receipts` (with and without a trust file) and `ledger
             scorecard` over a generated 10k-file ledger of 400 signed receipts

Quantiles are nearest-rank (docs/benchmark.md): the value at zero-based
index ceil(q * n) - 1 of the sorted samples.
"""

import argparse
import bisect
import datetime as dt
import gzip
import hashlib
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FORMAT = "bellwether-perf/1"
US_PER_DAY = 86_400_000_000

# ---------------------------------------------------------------- helpers


def nearest_rank(xs, q):
    s = sorted(xs)
    return s[max(0, math.ceil(q * len(s)) - 1)]


def sh(cmd, check=True, env=None, cwd=None):
    """Runs cmd; returns (wall ms, stdout, stderr, exit code)."""
    t = time.perf_counter()
    p = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=cwd)
    ms = (time.perf_counter() - t) * 1000.0
    if check and p.returncode != 0:
        sys.exit(f"failed ({p.returncode}): {' '.join(map(str, cmd))}\n{p.stderr.strip()}")
    return ms, p.stdout, p.stderr, p.returncode


def say(*a):
    print(*a, flush=True)


def iso_us(us):
    return dt.datetime.fromtimestamp(us / 1e6, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def duck_ts(us):
    return "TIMESTAMPTZ '" + dt.datetime.fromtimestamp(us / 1e6, tz=dt.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S.%f+00") + "'"


def time_cmd():
    """`/usr/bin/time` flag for peak RSS, and a parser for its output."""
    if sys.platform == "darwin":
        def parse(err):
            m = re.search(r"(\d+)\s+maximum resident set size", err)
            return int(m.group(1)) if m else None
        return ["/usr/bin/time", "-l"], parse

    def parse(err):
        m = re.search(r"Maximum resident set size \(kbytes\): (\d+)", err)
        return int(m.group(1)) * 1024 if m else None
    return ["/usr/bin/time", "-v"], parse


def peak_footprint(err):
    """macOS `/usr/bin/time -l`'s peak memory footprint (dirty memory, not
    mapped file pages), or None where it is not printed."""
    m = re.search(r"(\d+)\s+peak memory footprint", err)
    return int(m.group(1)) if m else None


def parse_duration(s):
    """A Rust `Duration` Debug string (12.3ms, 850µs, 1.2s) in ms."""
    m = re.fullmatch(r"([\d.]+)(ns|µs|us|ms|s)", s.strip())
    if not m:
        raise ValueError(f"not a duration: {s!r}")
    return float(m.group(1)) * {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1000.0}[m.group(2)]


# ------------------------------------------------------------ environment


def environment(a):
    def first(cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True).stdout.strip().splitlines()[0]
        except (OSError, IndexError):
            return None
    if sys.platform == "darwin":
        cpu = first(["sysctl", "-n", "machdep.cpu.brand_string"])
        cores = int(first(["sysctl", "-n", "hw.ncpu"]))
        mem = int(first(["sysctl", "-n", "hw.memsize"]))
        osv = f"macOS {first(['sw_vers', '-productVersion'])}"
    else:
        cpu = next((l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name")), platform.machine())
        cores = os.cpu_count()
        mem = next(int(l.split()[1]) * 1024 for l in open("/proc/meminfo") if l.startswith("MemTotal:"))
        osv = f"{platform.system()} {platform.release()}"
    commit = first(["git", "-C", REPO, "rev-parse", "--short=12", "HEAD"])
    # The chart rows this script writes itself (--showcase) do not make the
    # measured tree dirty.
    # Neither do documents edited while a run goes on: only code counts.
    dirty = bool(subprocess.run(["git", "-C", REPO, "status", "--porcelain", "--untracked-files=no", "--",
                                 ".", ":(exclude)docs", ":(exclude)*.md"],
                                capture_output=True, text=True).stdout.strip())
    # The binary's identity: cargo rebuilds whenever a Rust source or
    # manifest changes, so the binary matches the newest commit touching
    # them, provided none is modified in the tree.
    src = ["--", "crates/**/*.rs", "crates/*/Cargo.toml", "crates/*/build.rs", "Cargo.toml", "Cargo.lock"]
    tree = a.built_from or REPO
    sources = first(["git", "-C", tree, "log", "-1", "--format=%h", "--abbrev=12", *src])
    sources_dirty = bool(subprocess.run(["git", "-C", tree, "status", "--porcelain", *src],
                                        capture_output=True, text=True).stdout.strip())
    with open(a.bin, "rb") as f:
        sha = hashlib.sha256(f.read()).hexdigest()
    machine = f"{cpu}, {cores} cores, {mem // 2**30} GiB, {osv}, {'CI runner disk' if os.environ.get('GITHUB_ACTIONS') else 'local SSD'}"
    return {
        "date": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d"),
        "started_at": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "machine": machine,
        "cpu_model": cpu,
        "cores": cores,
        "memory_bytes": mem,
        "os": osv,
        "arch": platform.machine(),
        "commit": commit,
        "commit_dirty": dirty,
        "rustc": first(["rustc", "-V"]),
        "build": a.build_label,
        "binary": a.bin,
        "binary_sha256": sha,
        "built_from": sources,
        "built_from_dirty": sources_dirty,
        "duckdb_cli": first(["duckdb", "-no-agent", "--version"]),
        "storage": "local disk",
    }


class Cache:
    """Dropping caches between cold samples."""

    def __init__(self):
        self.cmd = None
        if sys.platform == "darwin":
            if subprocess.run(["sudo", "-n", "purge"], capture_output=True).returncode == 0:
                self.cmd = ["sudo", "-n", "purge"]
        elif sys.platform.startswith("linux"):
            c = ["sudo", "-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"]
            if subprocess.run(c, capture_output=True).returncode == 0:
                self.cmd = c
        if self.cmd:
            self.label = ("cold: new process, bellwether's cache directory removed, and the OS "
                          f"page cache dropped (`{' '.join(self.cmd[2:])}`) before each sample")
        else:
            self.label = ("cold (process): new process and bellwether's cache directory removed "
                          "before each sample; the OS page cache could not be dropped (needs root), "
                          "so file reads are served from memory")

    def drop(self):
        if self.cmd:
            subprocess.run(self.cmd, capture_output=True, check=True)


# ---------------------------------------------------------------- results


def external_cpu():
    """Cores busy in processes that are not this run or its children (the
    load average also counts this run's own worker threads)."""
    out = subprocess.run(["ps", "-Ao", "pid=,ppid=,pcpu="], capture_output=True, text=True).stdout
    procs = {}
    for line in out.splitlines():
        f = line.split()
        if len(f) == 3:
            procs[int(f[0])] = (int(f[1]), float(f[2]))
    me = os.getpid()

    def mine(pid):
        seen = 0
        while pid in procs and pid > 1 and seen < 64:
            if pid == me:
                return True
            pid, seen = procs[pid][0], seen + 1
        return False
    names = {}
    for line in subprocess.run(["ps", "-Ao", "pid=,comm="], capture_output=True, text=True).stdout.splitlines():
        f = line.split(None, 1)
        if len(f) == 2:
            names[int(f[0])] = f[1].strip()
    ext = [(cpu, public_name(names.get(pid, str(pid)))) for pid, (_, cpu) in procs.items()
           if not mine(pid) and cpu > 0]
    ext.sort(reverse=True)
    return sum(c for c, _ in ext) / 100.0, [f"{n} {c:.0f}%" for c, n in ext[:5]]


# Outside processes are named only when they are build or query tools; the
# rest of the desktop is "other process".
TOOLS = {"rustc", "cargo", "cargo-clippy", "clippy-driver", "rustdoc", "ld", "cc", "clang", "duckdb",
         "bellwether", "bellwether-mcp", "python3", "git", "kernel_task", "WindowServer"}


def public_name(comm):
    base = comm.strip("()").rsplit("/", 1)[-1]
    return base if base in TOOLS or base.startswith("bellwether") else "other process"


def scrub(text, a):
    """Machine-local paths in recorded commands, as the variables that name
    them, so a committed perf.json carries no home directory."""
    pairs = [(a.work, "$PERF_WORK")]
    if a.realdata:
        pairs += [(os.path.realpath(a.realdata), "$REALDATA_WORK"), (os.path.abspath(a.realdata), "$REALDATA_WORK")]
    pairs.append((os.path.expanduser("~"), "~"))
    for old, new in sorted(pairs, key=lambda p: -len(p[0])):
        text = text.replace(old, new)
    return text


def normalize(doc):
    """Applies this script's current classification to metrics an older
    run wrote: DuckDB's times and reconcile meter peaks are context."""
    for m in doc["metrics"]:
        if m.get("side") == "duckdb" or m.get("metric") == "meter peak state":
            m["direction"] = "none"


def keep_previous_lookups(old, context):
    """A lookups rerun keeps the attempt it replaces, as its warm plan p50s
    and its section record, so the two can be compared."""
    sec = old["environment"].get("sections", {}).get("lookups")
    plans = {m["id"]: m["value"] for m in old["metrics"]
             if m["id"].startswith("lookup/") and m["id"].endswith("/plan/warm/p50")}
    if sec and plans:
        context["lookups-previous-attempt"] = {"section": sec, "plan_warm_p50": plans}


class Perf:
    def __init__(self, a):
        self.a = a
        self.env = environment(a)
        self.cache = Cache()
        self.metrics = []
        self.context = {}
        self.sections = {}
        os.makedirs(a.out, exist_ok=True)
        old = os.path.join(a.out, "perf.json")
        if a.merge and os.path.exists(old):
            # Keep what other sections measured; replace what this run measures.
            doc = json.load(open(old))
            mine = tuple(p for s in a.run_sections if s != "reconcile" for p in SECTION_PREFIXES[s])
            if "reconcile" in a.run_sections:
                # Only the pairs (and, with --reconcile-budgets, the budgets)
                # this run reconciles are replaced; the one-pass search always
                # reruns.
                for d in a.reconcile_datasets:
                    base = f"reconcile/{RECONCILE_NAMES[d]}/"
                    if set(a.reconcile_budgets) == {l for l, _ in BUDGETS}:
                        mine += (base,)
                    else:
                        mine += tuple(f"{base}{l}" for l in a.reconcile_budgets) + (f"{base}smallest-one-pass",)
            normalize(doc)
            self.metrics = [m for m in doc["metrics"] if not m["id"].startswith(mine)]
            self.context = {k: v for k, v in doc["context"].items() if not k.startswith(mine)}
            self.sections = doc["environment"].get("sections", {})
            if "lookups" in a.run_sections:
                keep_previous_lookups(doc, self.context)
        spath = os.path.join(a.out, "samples.jsonl")
        if a.merge and os.path.exists(spath) and os.path.exists(old):
            # Drop the samples of what this run replaces.
            keep = [l for l in open(spath) if not json.loads(l)["id"].startswith(mine)]
            with open(spath, "w") as f:
                f.writelines(keep)
        elif not a.merge and os.path.exists(spath):
            os.remove(spath)
        self.samples = open(spath, "a")

    def section(self, name, start):
        """Records when a section ran, at which commit, and the machine's
        load around it."""
        load = round(os.getloadavg()[0], 2)
        if start:
            self.env = {**environment(self.a), "sections": self.sections}
            self.sections[name] = {"started_at": self.env["started_at"], "commit": self.env["commit"],
                                   "commit_dirty": self.env["commit_dirty"], "load_avg_1m_start": load}
            self.load_max = load
            self.ext_max, self.ext_top = external_cpu()
            say(f"section {name}: load {load}, outside this run {self.ext_max:.1f} cores "
                f"({', '.join(self.ext_top[:3])})")
            self.load_stop = threading.Event()

            self.ext_series = []

            def watch():
                while not self.load_stop.wait(5):
                    self.load_max = max(self.load_max, os.getloadavg()[0])
                    cores, top = external_cpu()
                    self.ext_series.append(cores)
                    if cores > self.ext_max:
                        self.ext_max, self.ext_top = cores, top
            self.load_thread = threading.Thread(target=watch, daemon=True)
            self.load_thread.start()
        else:
            self.load_stop.set()
            self.load_thread.join()
            self.sections[name]["finished_at"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            self.sections[name]["load_avg_1m_end"] = load
            self.sections[name]["load_avg_1m_max"] = round(max(self.load_max, load), 2)
            self.sections[name]["external_cpu_cores_max"] = round(self.ext_max, 2)
            self.sections[name]["external_top_at_max"] = self.ext_top
            xs = self.ext_series or [self.ext_max]
            # How long the outside load lasted, not only its peak.
            self.sections[name]["external_cpu_samples"] = len(xs)
            self.sections[name]["external_cpu_cores_p50"] = round(nearest_rank(xs, 0.5), 2)
            self.sections[name]["external_cpu_share_over_2_cores"] = round(sum(x > 2 for x in xs) / len(xs), 3)
            if self.ext_max > self.a.max_load:
                say(f"WARNING: {name}: processes outside this run used up to {self.ext_max:.1f} cores "
                    f"(limit {self.a.max_load}; at the peak: {', '.join(self.ext_top)}); rerun this section on a quiet machine before publishing it")
        self.env["sections"] = self.sections

    def sample(self, mid, values, **extra):
        self.samples.write(scrub(json.dumps({"id": mid, "values": values, **extra}), self.a) + "\n")
        self.samples.flush()

    def metric(self, mid, value, unit, direction, exactness="measured", **fields):
        """direction: lower (a latency: budget = x 1.25), higher (a
        throughput: budget = / 1.25), or none (context, no budget)."""
        self.metrics.append({"id": mid, "value": value, "unit": unit, "direction": direction,
                             "exactness": exactness, **fields})

    def quantiles(self, mid, values, unit="ms", direction="lower", **fields):
        self.sample(mid, values, **fields)
        for q, name in [(0.5, "p50"), (0.95, "p95")]:
            self.metric(f"{mid}/{name}", round(nearest_rank(values, q), 3), unit, direction,
                        stat=name, n=len(values), **fields)

    def write(self):
        for sec in self.sections.values():
            sec["external_top_at_max"] = [
                f"{public_name(e.rsplit(' ', 1)[0])} {e.rsplit(' ', 1)[1]}" if " " in e else e
                for e in sec.get("external_top_at_max", [])]
        doc = {"format": FORMAT, "environment": self.env, "cache": {"warm": "warm: the same call ran "
               "just before, so bellwether's cache directory and the OS page cache hold its inputs",
               "cold": self.cache.label}, "context": self.context, "metrics": self.metrics}
        path = os.path.join(self.a.out, "perf.json")
        with open(path, "w") as f:
            f.write(scrub(json.dumps(doc, indent=1), self.a) + "\n")
        if self.a.showcase:
            rows, queries = self.showcase_rows()
            os.makedirs(os.path.dirname(os.path.abspath(self.a.showcase)), exist_ok=True)
            with open(self.a.showcase, "w") as f:
                json.dump({"source": "perf", "generated_by": "scripts/perf.py (via scripts/perf.sh)",
                           "charts": charts(rows, self.process_start()), "queries": queries, "rows": rows}, f, indent=1)
                f.write("\n")
        say(f"wrote {path} ({len(self.metrics)} metrics)")

    def process_start(self):
        return next((m["value"] for m in self.metrics
                     if m["id"] == "process/bellwether--version/warm/p50"), None)

    def showcase_rows(self):
        """Chart rows for the README's results renderer: one object per
        value with dataset, query, side, metric, value, unit, exactness,
        machine, build, and date, and one unit per metric."""
        env = self.env
        rustc = (env.get("rustc") or "").split(" (")[0]
        built = f"sources at {env['built_from']}" + (" (modified)" if env["built_from_dirty"] else "")
        builds = {
            "bellwether": f"bellwether release build of {built} ({rustc}; sha256 {env['binary_sha256'][:12]})",
            "full-scan": f"bellwether-bench byte-counting reader in the same binary, {built}",
            "duckdb": f"DuckDB {(env.get('duckdb_cli') or '').strip()}",
        }
        rows, queries = [], {}

        def row(dataset, query, side, metric, value, unit, exactness):
            rows.append({"dataset": dataset, "query": query, "side": side, "metric": metric,
                         "value": value, "unit": unit, "exactness": exactness,
                         "machine": env["machine"],
                         "build": builds["duckdb" if side == "duckdb" else
                                         "full-scan" if side == "full-scan" else "bellwether"],
                         "date": env["date"]})

        full = {}
        for m in self.metrics:
            parts = m["id"].split("/")
            if parts[0] == "lookup" and parts[2] == "full-scan":
                full[(parts[1], parts[3], parts[4])] = m["value"]
        sides = {"plan": "bellwether-plan", "e2e": "bellwether-end-to-end", "duckdb": "duckdb"}
        for m in self.metrics:
            parts, v = m["id"].split("/"), m["value"]
            if parts[0] == "lookup" and parts[2] != "full-scan":
                ds, name, k, what, cache, stat = parts[1:7]
                qid = f"{name}-{k}"
                row(ds, qid, sides[what], f"lookup_{stat}_ms_{cache}", v, "ms", "measured")
                if what == "plan":
                    row(ds, qid, "full-scan", f"lookup_{stat}_ms_{cache}", full[(ds, cache, stat)],
                        "ms", "measured")
                ctx = self.context.get(f"lookup/{ds}/{name}/{k}")
                if ctx and what == "plan" and cache == "warm" and stat == "p50":
                    queries[qid] = {"text": QUERY_TEXT[name].format(k=k[1:]), "keys": int(k[1:])}
                    fs = self.context[f"dataset/{ds}"]["full_scan"]
                    for side, rg, b in (("bellwether-plan", ctx["planned"]["row_groups"], ctx["planned"]["bytes"]),
                                        ("full-scan", fs["row_groups"], fs["bytes_fetched"])):
                        row(ds, qid, side, "row_groups", rg, "row groups", "exact")
                        row(ds, qid, side, "bytes", b, "bytes", "exact")
            elif parts[0] == "build" and len(parts) == 4 and parts[3] == "row-groups-per-s":
                row(parts[1], "index", f"bellwether-threads-{parts[2][1:]}", "build_row_groups_per_s",
                    v, "row groups/s", "measured")
            elif parts[0] == "build" and len(parts) == 4 and parts[3] == "peak-rss":
                row(parts[1], "index", f"bellwether-threads-{parts[2][1:]}", "build_peak_rss_bytes",
                    v, "bytes", "measured")
            elif parts[0] == "reconcile" and len(parts) == 4 and parts[3] == "rows-per-s-per-side":
                row(parts[1], "reconcile", f"bellwether-budget-{parts[2]}",
                    "reconcile_rows_per_s_per_side", v, "rows/s", "measured")
            elif parts[0] == "reconcile" and len(parts) == 4 and parts[3] == "passes":
                row(parts[1], "reconcile", f"bellwether-budget-{parts[2]}", "reconcile_passes", v,
                    "passes", "exact")
            elif parts[0] == "replay":
                row(m["dataset"], parts[1], "bellwether", "replay_p50_run_ms", v, "ms", "measured")
            elif parts[0] == "observe" and parts[-1] == "p50":
                side = "bellwether-cli" if parts[1].startswith("cli") else "bellwether-facade"
                row("replay-small", "why-no-workload", side, "observe_p50_ms", v, "ms", "measured")
            elif parts[0] == "mcp":
                row("replay-small", "why-no-workload", f"bellwether-mcp-{parts[2]}", "observe_p50_ms",
                    v, "ms", "measured")
            elif parts[0] == "why":
                row(parts[1], "why-preregistered", f"bellwether-{parts[3]}", f"why_{parts[4]}_ms", v,
                    "ms", "measured")
            elif parts[0] == "resident":
                row("in-process UInt64 keys", parts[1], "bellwether", "resident_bytes_per_key", v,
                    "bytes", "measured")
        return rows, queries


def charts(rows, start=None):
    """The charts the README draws from these rows, each with a takeaway
    sentence computed from the numbers."""
    def by(metric):
        out = {}
        for r in rows:
            if r["metric"] == metric and isinstance(r["value"], (int, float)):
                out[(r["dataset"], r["query"], r["side"])] = r["value"]
        return out

    out = []
    look = by("lookup_p50_ms_warm")
    groups = sorted({(d, q) for d, q, _ in look})
    complete = [(d, q) for d, q in groups
                if all((d, q, s) in look for s in ("bellwether-end-to-end", "full-scan", "duckdb"))]
    if complete:
        n = len(complete)
        beat_scan = sum(look[(d, q, "bellwether-end-to-end")] < look[(d, q, "full-scan")] for d, q in complete)
        beat_duck = sum(look[(d, q, "bellwether-end-to-end")] < look[(d, q, "duckdb")] for d, q in complete)
        plan = sorted(look[(d, q, "bellwether-plan")] for d, q in complete if (d, q, "bellwether-plan") in look)
        out.append({
            "name": "lookup-latency", "metric": "lookup_p50_ms_warm", "log": True,
            "title": "Lookup wall time, warm, p50 of 20 (plan, plan + read, full scan, DuckDB)",
            "sides": ["bellwether-plan", "bellwether-end-to-end", "full-scan", "duckdb"],
            "takeaway": (f"Planning with the index and reading only the planned row groups beat a full scan "
                         f"in {beat_scan} of {n} lookups and DuckDB in {beat_duck} of {n}; the plan alone "
                         f"(one bellwether process) took {nearest_rank(plan, 0.5):.0f} ms at the median"
                         + (f", of which {start:.0f} ms is starting any bellwether process." if start else ".")),
        })
    build = by("build_row_groups_per_s")
    parts = []
    for ds in sorted({d for d, _, _ in build}):
        one = build.get((ds, "index", "bellwether-threads-1"))
        allt = build.get((ds, "index", "bellwether-threads-all"))
        best = max((v, s) for (d, _, s), v in build.items() if d == ds)
        if one and allt:
            parts.append(f"{ds} went from {one:,.0f} row groups per second on one thread to {allt:,.0f} on "
                         f"all ({allt / one:.1f}x; best {best[0]:,.0f} at "
                         f"{best[1].removeprefix('bellwether-threads-')})")
    if parts:
        out.append({
            "name": "build", "metric": "build_row_groups_per_s", "log": True,
            "title": "Index build throughput by --threads, row groups per second",
            "sides": [f"bellwether-threads-{t}" for t in ("1", "2", "4", "8", "all")],
            "takeaway": "; ".join(parts) + ".",
        })
    rec = by("reconcile_rows_per_s_per_side")
    passes = by("reconcile_passes")
    if rec:
        ds = sorted({d for d, _, _ in rec})
        parts = []
        for d in ds:
            lo = rec.get((d, "reconcile", "bellwether-budget-64M"))
            hi = rec.get((d, "reconcile", "bellwether-budget-8G"))
            p64 = passes.get((d, "reconcile", "bellwether-budget-64M"))
            if lo and hi:
                parts.append(f"{d} compared {hi:,.0f} rows per second per side at 8 GiB and {lo:,.0f} at "
                             f"64 MiB ({p64} passes)")
        out.append({
            "name": "reconcile", "metric": "reconcile_rows_per_s_per_side", "log": False,
            "title": "Reconcile throughput by memory budget, rows per second per side",
            "sides": [f"bellwether-budget-{b}" for b in ("64M", "256M", "1G", "8G")],
            "takeaway": "; ".join(parts) + "." if parts else "Reconcile throughput by memory budget.",
        })
    obs = by("observe_p50_ms")
    if obs:
        worst = max(obs.values())
        out.append({
            "name": "observe", "metric": "observe_p50_ms", "log": False,
            "title": "Observe tier (why, no workload) on replay-small, p50",
            "sides": ["bellwether-facade", "bellwether-mcp-in-process", "bellwether-mcp-stdio", "bellwether-cli"],
            "takeaway": (f"Every way of asking why with no workload answered within {worst:.0f} ms at the "
                         f"median, against the 100 ms observe budget"
                         + (" (the CLI figure includes process start)." if ("replay-small", "why-no-workload", "bellwether-cli") in obs else ".")),
        })
    return out


RECONCILE_NAMES = {"ledger": "ledger-small", "replay": "replay-small", "taxi": "taxi"}

# What each section owns in perf.json (metric ids and context keys).
SECTION_PREFIXES = {
    "lookups": ("lookup/", "process/", "dataset/", "keys/", "skipped"),
    "build": ("build/",),
    "reconcile": ("reconcile/",),
    "latency": ("replay/", "observe/", "mcp/", "resident/"),
    "why": ("why/",),
    "profiles": ("profile/",),
    "ledger": ("ledger/",),
}


# --------------------------------------------------------------- datasets


class Dataset:
    def __init__(self, name, data, idx, key, ts, glob, specs, partitioning=None):
        self.name, self.data, self.idx, self.key, self.ts = name, data, idx, key, ts
        self.glob, self.specs, self.partitioning = glob, specs, partitioning
        self.span = None  # (min us, max us) of the time column
        self.symbols = None  # distinct key count, when known

    def index_args(self, idx=None, threads=None):
        args = ["index", self.data, "--index", idx or self.idx]
        if self.partitioning:
            args += ["--partitioning", self.partitioning]
        for s in self.specs:
            args += ["--key", s]
        if threads:
            args += ["--threads", str(threads)]
        return args


def duck(sql, check=True):
    env = {k: v for k, v in os.environ.items() if k != "AI_AGENT"}
    return sh(["duckdb", "-no-agent", "-init", "/dev/null", "-csv", "-noheader", "-c", sql],
              check=check, env=env)[1].strip()


def market_dataset(p, name, extra):
    root = os.path.join(p.a.work, name)
    if not os.path.exists(os.path.join(root, "workload", "truth.json")):
        shutil.rmtree(root, ignore_errors=True)
        sh([p.a.bin, "bench", "market", "--out", root, *extra])
    data = os.path.realpath(os.path.join(root, "data"))
    d = Dataset(name, data, os.path.join(p.a.work, name + "-idx"), "symbol", "ts",
                os.path.join(data, "*", "*.parquet"),
                ["exact:symbol@ts", "range:ts", "temporal:symbol@ts"], "hive")
    lo, hi, n = duck(f"SELECT epoch_us(min(ts)), epoch_us(max(ts)), count(DISTINCT symbol) "
                     f"FROM read_parquet('{d.glob}')").split(",")
    d.span, d.symbols = (int(lo), int(hi)), int(n)
    d.command = f"bellwether bench market --out {name} {' '.join(extra)}".strip()
    return d


def gh_dataset(p):
    src = os.path.join(p.a.realdata, "gh")
    if not os.path.isdir(src):
        say(f"skipping GH events: no {src} (run scripts/realdata.sh, or pass --realdata)")
        return None
    data = os.path.realpath(src)
    d = Dataset("gh-events", data, os.path.join(p.a.work, "gh-events-idx"), "repo_id", "created_at",
                os.path.join(data, "*.parquet"),
                ["exact:repo_id@created_at", "range:created_at", "temporal:repo_id@created_at"])
    lo, hi, n = duck(f"SELECT epoch_us(min(created_at)), epoch_us(max(created_at)), "
                     f"count(DISTINCT repo_id) FROM read_parquet('{d.glob}')").split(",")
    d.span, d.symbols = (int(lo), int(hi)), int(n)
    d.command = "GH Archive 2024-01-01 hours 00-11 flattened by scripts/realdata.sh"
    return d


def ledger_dataset(p, name, args):
    root = os.path.join(p.a.work, name)
    if not os.path.exists(os.path.join(root, "workload.json")):
        shutil.rmtree(root, ignore_errors=True)
        sh([p.a.bin, "bench", "generate", "--out", root, *args])
        wl = ["--preregistered"] if "--rung" in args else ["--runs", "48", "--p50", "40", "--p95",
                                                            "20000", "--max", "60000"]
        sh([p.a.bin, "bench", "workload", "--corpus", root, *wl])
    data = os.path.realpath(os.path.join(root, "data"))
    d = Dataset(name, data, os.path.join(p.a.work, name + "-idx"), "entity_id", "version_ts",
                os.path.join(data, "*", "*.parquet"),
                ["exact:entity_id", "temporal:entity_id@version_ts"], "hive")
    d.root = os.path.realpath(root)
    d.command = f"bellwether bench generate --out {name} {' '.join(args)}"
    return d


def key_file(p, d, k):
    """k distinct keys of d, chosen by a seeded hash (not DuckDB's thread
    schedule), as a one-column Parquet file."""
    path = os.path.join(p.a.work, "keys", f"{d.name}-{k}.parquet")
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        duck(f"COPY (SELECT {d.key} FROM (SELECT DISTINCT {d.key} FROM read_parquet('{d.glob}') "
             f"WHERE {d.key} IS NOT NULL) ORDER BY hash({d.key}, 42), {d.key} LIMIT {k}) "
             f"TO '{path}' (FORMAT PARQUET)")
    return path


def ensure_index(p, d):
    if not os.path.exists(os.path.join(d.idx, "manifest.json")):
        shutil.rmtree(d.idx, ignore_errors=True)
        sh([p.a.bin, *d.index_args()])


# ---------------------------------------------------------------- lookups


QUERY_TEXT = {
    "locate-window-1d": "locate --window: {k} keys, the day before the midpoint instant",
    "locate-window-30d": "locate --window: {k} keys, the 30 days before the midpoint instant",
    "latest": "latest: the newest version of {k} keys",
    "asof": "asof --at the midpoint instant: {k} keys",
    "asof-lookback-1d": "asof --at the midpoint --lookback 1d: {k} keys",
    "asof-lookback-30d": "asof --at the midpoint --lookback 30d: {k} keys",
}


def mid_hour(span):
    """The midpoint of a time span, rounded down to a whole UTC hour."""
    lo, hi = span
    return (lo + (hi - lo) // 2) // 3_600_000_000 * 3_600_000_000


def lookups_for(d):
    """(name, CLI args after --keys FILE, DuckDB WHERE/QUALIFY builder)."""
    lo, hi = d.span
    at = mid_hour(d.span)
    out = []
    for days in (1, 30):
        w0 = max(lo, at - days * US_PER_DAY)
        out.append((f"locate-window-{days}d",
                    ["locate", "--window", f"{iso_us(w0)}..{iso_us(at)}", "--ts", d.ts],
                    f"{d.ts} BETWEEN {duck_ts(w0)} AND {duck_ts(at)}", None,
                    f"[{iso_us(w0)}, {iso_us(at)}]"))
    out.append(("latest", ["latest"], "TRUE", f"{d.ts} = max({d.ts}) OVER (PARTITION BY {d.key})",
                "newest version per key"))
    out.append(("asof", ["asof", "--at", iso_us(at)], f"{d.ts} <= {duck_ts(at)}",
                f"{d.ts} = max({d.ts}) OVER (PARTITION BY {d.key})", f"at {iso_us(at)}"))
    for days in (1, 30):
        w0 = at - days * US_PER_DAY
        out.append((f"asof-lookback-{days}d",
                    ["asof", "--at", iso_us(at), "--lookback", f"{days}d", "--ts", d.ts],
                    f"{d.ts} BETWEEN {duck_ts(max(lo, w0))} AND {duck_ts(at)}", None,
                    f"at {iso_us(at)}, lookback {days}d"))
    return out


def duck_sql(d, keys, where, qualify):
    sel = (f"SELECT hash(t) h FROM read_parquet('{d.glob}') t WHERE {d.key} IN "
           f"(SELECT {d.key} FROM read_parquet('{keys}')) AND {where}")
    if qualify:
        sel += f" QUALIFY {qualify}"
    return f"SELECT count(*), sum(h) FROM ({sel})"


def duck_times(sql, reps, discard=1):
    """DuckDB's own .timer query times (ms) for reps runs in one fresh
    process, after `discard` runs whose times are dropped, and the result
    row. Warm: discard=1. Cold: reps=1, discard=0, so the timed query is the
    first one the process runs."""
    script = ".timer on\n" + (sql + ";\n") * (reps + discard)
    env = {k: v for k, v in os.environ.items() if k != "AI_AGENT"}
    p = subprocess.run(["duckdb", "-no-agent", "-init", "/dev/null", "-csv", "-noheader"],
                       input=script, capture_output=True, text=True, env=env)
    if p.returncode != 0 or p.stderr.strip():
        sys.exit(f"duckdb failed: {p.stderr.strip()}\n{sql}")
    times = [float(x) * 1000 for x in re.findall(r"Run Time \(s\): real ([\d.]+)", p.stdout)]
    rows = [l for l in p.stdout.splitlines() if not l.startswith("Run Time")]
    if len(times) != reps + discard:
        sys.exit(f"duckdb printed {len(times)} timings for {reps + discard} queries")
    return times[discard:], rows[0]


def read_ms(p, args):
    _, out, _, _ = sh([p.a.bin, "bench", "read", *args, "--json"])
    return json.loads(out)["read"]


def section_lookups(p, datasets):
    W, C = p.a.warm, p.a.cold
    bw = p.a.bin
    # Process start, for reading plan times (each includes one).
    t = [sh([bw, "--version"])[0] for _ in range(W)]
    p.quantiles("process/bellwether--version/warm", t, metric="process start", cache="warm",
                command="bellwether --version")
    for d in datasets:
        ensure_index(p, d)
        say(f"lookups: {d.name}")
        # Full scan of the same files through the same reader.
        warm = [read_ms(p, ["--all", d.data])["total_ms"] for _ in range(W + 1)][1:]
        cold = []
        for _ in range(C):
            p.cache.drop()
            cold.append(read_ms(p, ["--all", d.data])["total_ms"])
        full = read_ms(p, ["--all", d.data])
        p.context[f"dataset/{d.name}"] = {"data": d.data, "command": d.command, "specs": d.specs,
                                          "span": [iso_us(d.span[0]), iso_us(d.span[1])],
                                          "distinct_keys": d.symbols, "full_scan": full}
        for cache, xs in (("warm", warm), ("cold", cold)):
            p.quantiles(f"lookup/{d.name}/full-scan/{cache}", xs, dataset=d.name, query="full scan",
                        side="full-scan", metric="end-to-end", cache=cache,
                        command=f"bellwether bench read --all {d.data}")
        for k in p.a.key_sets:
            if d.symbols is not None and k > d.symbols:
                p.context.setdefault("skipped", []).append(
                    f"{d.name}: {k} keys asked, the corpus has {d.symbols} distinct keys")
                continue
            keys = key_file(p, d, k)
            # The key sets depend on DuckDB's hash(); record them so a run on
            # another DuckDB can use the same keys.
            p.context[f"keys/{d.name}/k{k}"] = duck(
                f"SELECT string_agg(CAST({d.key} AS VARCHAR), ',' ORDER BY {d.key}) FROM read_parquet('{keys}')").strip('"').split(",")
            for name, args, where, qualify, what in lookups_for(d):
                lookup_one(p, d, k, keys, name, args, where, qualify, what)
    return p


def lookup_one(p, d, k, keys, name, args, where, qualify, what):
    W, C = p.a.warm, p.a.cold
    bw = p.a.bin
    cmd = [bw, args[0], d.data, "--keys", keys, *args[1:], "--index", d.idx, "--json"]
    plan_file = os.path.join(p.a.work, "plans", f"{d.name}-{k}-{name}.json")
    os.makedirs(os.path.dirname(plan_file), exist_ok=True)
    _, out, _, _ = sh(cmd)
    with open(plan_file, "w") as f:
        f.write(out)
    v = json.loads(out)
    mid = f"lookup/{d.name}/{name}/k{k}"
    common = dict(dataset=d.name, query=name, keys=k)
    plan_w = [sh(cmd)[0] for _ in range(W)]
    read_w = [read_ms(p, ["--plan", plan_file])["total_ms"] for _ in range(W)]
    plan_c, read_c = [], []
    for _ in range(C):
        p.cache.drop()
        shutil.rmtree(os.path.join(d.idx, "cache"), ignore_errors=True)
        plan_c.append(sh(cmd)[0])
        p.cache.drop()
        read_c.append(read_ms(p, ["--plan", plan_file])["total_ms"])
    r = read_ms(p, ["--plan", plan_file])
    printable = "bellwether " + " ".join(str(x) for x in cmd[1:])
    for cache, pl, rd in (("warm", plan_w, read_w), ("cold", plan_c, read_c)):
        p.quantiles(f"{mid}/plan/{cache}", pl, side="bellwether", metric="plan", cache=cache,
                    command=printable, **common)
        p.quantiles(f"{mid}/e2e/{cache}", [a + b for a, b in zip(pl, rd)], side="bellwether",
                    metric="end-to-end", cache=cache,
                    command=printable + f" && bellwether bench read --plan {os.path.basename(plan_file)}",
                    **common)
    sql = duck_sql(d, keys, where, qualify)
    dw, row = duck_times(sql, W)
    dc = []
    for _ in range(C):
        p.cache.drop()
        dc.append(duck_times(sql, 1, discard=0)[0][0])
    for cache, xs in (("warm", dw), ("cold", dc)):
        # DuckDB is the baseline, not code this repository can regress: its
        # times are context (direction "none"), never budgeted.
        p.quantiles(f"{mid}/duckdb/{cache}", xs, direction="none", side="duckdb", metric="end-to-end",
                    cache=cache, command=f"duckdb .timer: {sql}", **common)
    p.context[mid] = {"what": what, "planned": v["planned"], "row_groups_total": v["row_groups_total"],
                      "files_total": v["files_total"], "read": r, "duckdb_rows": row.split(",")[0],
                      "command": printable}
    say(f"  {name:22s} k={k:<5d} plan p50 {nearest_rank(plan_w, .5):7.1f} ms  read p50 "
        f"{nearest_rank(read_w, .5):7.1f} ms  duckdb p50 {nearest_rank(dw, .5):7.1f} ms  "
        f"planned {v['planned']['row_groups']}/{v['row_groups_total']} row groups")


# ------------------------------------------------------------------ build


def section_build(p, datasets):
    tflag, rss = time_cmd()
    cores = os.cpu_count()
    threads = sorted({t for t in (1, 2, 4, 8) if t <= cores} | {cores})
    for d in datasets:
        say(f"build: {d.name}")
        idx = os.path.join(p.a.work, "build-idx", d.name)
        # Baseline process: an index that is already complete (nothing to do).
        shutil.rmtree(idx, ignore_errors=True)
        sh([p.a.bin, *d.index_args(idx=idx)])
        _, _, err, _ = sh([*tflag, p.a.bin, *d.index_args(idx=idx)])
        base = rss(err)
        p.metric(f"build/{d.name}/noop-peak-rss", base, "bytes", "none", dataset=d.name,
                 metric="peak RSS, index already complete", command="bellwether " + " ".join(d.index_args(idx="IDX")))
        for t in threads:
            walls, peaks, out = [], [], None
            for _ in range(p.a.build_reps):
                shutil.rmtree(idx, ignore_errors=True)
                ms, o, err, _ = sh([*tflag, p.a.bin, *d.index_args(idx=idx, threads=t), "--json"])
                walls.append(ms)
                peaks.append(rss(err))
                out = json.loads(o)
            wall = nearest_rank(walls, 0.5)
            rgs = out["row_groups_read"]
            label = "all" if t == cores else str(t)
            common = dict(dataset=d.name, query="index", threads=label,
                          command="bellwether " + " ".join(d.index_args(idx="IDX", threads=t)))
            p.sample(f"build/{d.name}/t{label}/wall", walls, **common)
            p.metric(f"build/{d.name}/t{label}/wall/p50", round(wall, 1), "ms", "lower", n=len(walls),
                     metric="wall", stat="p50", **common)
            p.metric(f"build/{d.name}/t{label}/row-groups-per-s", round(rgs / (wall / 1000), 1),
                     "row groups/s", "higher", n=len(walls), metric="throughput", **common)
            peak = max(peaks)
            p.metric(f"build/{d.name}/t{label}/peak-rss", peak, "bytes", "lower", metric="peak RSS",
                     **common)
            p.metric(f"build/{d.name}/t{label}/per-worker-rss", (peak - base) // t, "bytes", "lower",
                     metric="peak RSS over the no-op baseline, per worker", **common)
            p.metric(f"build/{d.name}/t{label}/segments", out["segments_written"], "segments", "none",
                     exactness="exact", metric="segments written", **common)
            p.context[f"build/{d.name}"] = {"row_groups": rgs, "specs": d.specs}
            say(f"  threads {label:>3}: {wall:8.0f} ms  {rgs / (wall / 1000):8.0f} rg/s  peak RSS "
                f"{peak / 2**20:6.0f} MiB  segments {out['segments_written']}")
        shutil.rmtree(idx, ignore_errors=True)


# -------------------------------------------------------------- reconcile


BUDGETS = [("64M", 64 << 20), ("256M", 256 << 20), ("1G", 1 << 30), ("8G", 8 << 30)]


def reconcile_pairs(p, ledger):
    pairs = []
    if ledger:
        right = ledger.data + "-copy"
        if not os.path.isdir(right):
            if sys.platform == "darwin":
                sh(["cp", "-c", "-R", ledger.data, right])
            else:
                sh(["cp", "-R", ledger.data, right])
        pairs.append((ledger.name, ledger.data, right,
                      ["--key", "entity_id", "--latest-by", "version_ts", "--totals", "amount",
                       "--group-by", "status"], "a byte-for-byte copy"))
    taxi = os.path.join(p.a.realdata, "taxi")
    taxi_duck = os.path.join(p.a.realdata, "taxi_duck")
    if os.path.isdir(taxi) and os.path.isdir(taxi_duck):
        pairs.append(("taxi", os.path.realpath(taxi), os.path.realpath(taxi_duck),
                      ["--key", "VendorID,tpep_pickup_datetime,PULocationID,fare_amount",
                       "--latest-by", "tpep_dropoff_datetime", "--totals", "fare_amount:dec(2)",
                       "--group-by", "VendorID"], "the same months rewritten by DuckDB"))
    else:
        say(f"skipping taxi reconcile: no {taxi} and {taxi_duck}")
    return pairs


def reconcile_once(p, name, left, right, args, budget):
    tflag, rss = time_cmd()
    idx = os.path.join(p.a.work, "reconcile-idx", name)
    cmd = [p.a.bin, "reconcile", left, right, *args, "--memory-budget", str(budget), "--index", idx,
           "--json"]
    ms, out, err, code = sh([*tflag, *cmd], check=False)
    if code not in (0, 1):
        sys.exit(f"reconcile failed ({code}): {err.strip()}")
    v = json.loads(out)
    mem = v["outcome"]["memory"]
    return ms, v, mem, (rss(err), peak_footprint(err)), "bellwether " + " ".join(cmd[1:-3] + ["--json"])


# The verify README's stated bound (ADR 018), less the fingerprint lists:
# the budget, one decoded batch, and ParquetSource's footer cache.
FOOTER_CACHE = 256 << 20


def stated_bound(mem):
    return mem["budget_bytes"] + mem["peak_batch_bytes"] + FOOTER_CACHE


def section_reconcile(p, ledger):
    for name, left, right, args, what in reconcile_pairs(p, ledger):
        if name.split("-")[0] not in p.a.reconcile_datasets and name not in p.a.reconcile_datasets:
            say(f"reconcile: {name} not selected (--reconcile-datasets)")
            continue
        say(f"reconcile: {name}")
        results = {}
        one_pass_ms = None
        # Largest budget first: its single pass predicts what the smaller
        # budgets cost, and one whose predicted wall time is past the cap is
        # recorded as not run rather than run for hours.
        for label, b in reversed([x for x in BUDGETS if x[0] in p.a.reconcile_budgets]):
            if one_pass_ms and name in results.get("_need", {}):
                need = results["_need"][name]
                predicted = math.ceil(need / b)
                if predicted * one_pass_ms > p.a.reconcile_cap_s * 1000:
                    p.context[f"reconcile/{name}/{label}"] = {
                        "not_run": f"about {predicted} passes at {label} (the one-pass state is "
                                   f"{need / 2**20:,.0f} MiB as budgeted), about "
                                   f"{predicted * one_pass_ms / 60000:,.0f} min; past the "
                                   f"{p.a.reconcile_cap_s // 60} min cap", "what": what}
                    say(f"  {label:>5}: not run, about {predicted} passes, "
                        f"{predicted * one_pass_ms / 60000:,.0f} min")
                    continue
            ms, v, mem, (peak, foot), cmd = reconcile_once(p, name, left, right, args, label)
            rows = [s["rows"] for s in v["outcome"]["sides"]]
            results[label] = (mem["passes"], b)
            if mem["passes"] == 1 and one_pass_ms is None:
                one_pass_ms = ms
                # Budget the planner needs for one pass is at least the
                # state it held; the bisection below measures the real need.
                results["_need"] = {name: mem["peak_state_bytes"]}
            common = dict(dataset=name, query="reconcile", budget=label, n=1, command=cmd)
            p.metric(f"reconcile/{name}/{label}/wall", round(ms, 1), "ms", "lower", metric="wall", **common)
            p.metric(f"reconcile/{name}/{label}/rows-per-s-per-side", round(rows[0] / (ms / 1000)),
                     "rows/s", "higher", metric="rows per second per side", **common)
            p.metric(f"reconcile/{name}/{label}/passes", mem["passes"], "passes", "none",
                     exactness="exact", metric="passes", **common)
            # The meter's peak is bounded by the budget by construction:
            # context, not a budget of its own.
            p.metric(f"reconcile/{name}/{label}/meter-peak", mem["peak_state_bytes"], "bytes", "none",
                     exactness="exact", metric="meter peak state", **common)
            # Peak memory is budgeted, but never above the stated bound: a
            # run past it is a defect, not a baseline (perf-budgets.py caps).
            bound = stated_bound(mem)
            p.metric(f"reconcile/{name}/{label}/peak-rss", peak, "bytes", "lower", metric="peak RSS",
                     bound_bytes=bound, **common)
            if foot is not None:
                p.metric(f"reconcile/{name}/{label}/peak-footprint", foot, "bytes", "lower",
                         metric="peak memory footprint (dirty memory, /usr/bin/time -l)", bound_bytes=bound,
                         **common)
            p.context[f"reconcile/{name}/{label}"] = {"memory": mem, "rows": rows, "pass": v["outcome"]["pass"],
                                                       "counts": v["outcome"]["counts"], "what": what}
            say(f"  {label:>5}: {ms:9.0f} ms  passes {mem['passes']:4d}  meter peak "
                f"{mem['peak_state_bytes'] / 2**20:8.1f} MiB  RSS {peak / 2**20:7.0f} MiB  "
                f"{rows[0] / (ms / 1000):10.0f} rows/s/side")
        results.pop("_need", None)
        ones = [b for passes, b in results.values() if passes == 1]
        floor_mem = None
        above = []
        if ones:
            floor_mem = p.context[f"reconcile/{name}/{[l for l, b in BUDGETS if b == min(ones)][0]}"]["memory"]
        else:
            # No budget run gave one pass: double past the largest, up to half
            # the machine's memory, so the one-pass budget is still found.
            b = max(b for _, b in results.values()) * 2
            limit = p.env["memory_bytes"] // 2
            while b <= limit:
                ms, _, mem, (peak, foot), _ = reconcile_once(p, name, left, right, args, f"{b >> 20}M")
                above.append({"budget_mib": b >> 20, "passes": mem["passes"], "meter_peak": mem["peak_state_bytes"],
                              "wall_ms": round(ms), "peak_rss": peak, "peak_footprint": foot})
                say(f"  {b >> 20:>5}M: {ms:9.0f} ms  passes {mem['passes']:4d}  meter peak "
                    f"{mem['peak_state_bytes'] / 2**20:8.1f} MiB  RSS {peak / 2**20:7.0f} MiB")
                results[f"{b >> 20}M"] = (mem["passes"], b)
                if mem["passes"] == 1:
                    ones, floor_mem = [b], mem
                    break
                b *= 2
            if not ones:
                p.context[f"reconcile/{name}/smallest-one-pass"] = {
                    "none": f"more than one pass at every budget run, up to {limit >> 20} MiB", "above": above}
                continue
        # The smallest budget with one pass, by bisection on MiB, between the
        # one-pass run's state (a budget below what one pass holds cannot be
        # one pass) and the smallest budget that gave one pass.
        hi = min(ones) >> 20
        more = [b for passes, b in results.values() if passes > 1 and b < min(ones)]
        lo = max([b >> 20 for b in more] + [max(1, floor_mem["peak_state_bytes"] >> 20) - 1])
        probes = []
        while hi - lo > max(1, hi // 10):
            mid = (lo + hi) // 2
            _, _, mem, _, _ = reconcile_once(p, name, left, right, args, f"{mid}M")
            probes.append((mid, mem["passes"], mem["peak_state_bytes"]))
            if mem["passes"] == 1:
                hi = mid
            else:
                lo = mid
        mem = next((m for b, ps, m in [(x[0], x[1], {"peak_state_bytes": x[2]}) for x in probes] if b == hi), None)
        p.metric(f"reconcile/{name}/smallest-one-pass-budget", hi << 20, "bytes", "none",
                 dataset=name, metric="smallest budget with passes == 1 (within 10%)")
        p.context[f"reconcile/{name}/smallest-one-pass"] = {
            "budget_mib": hi, "probes": probes, "bounds_mib": [lo, hi], "above": above,
            "memory_at_budget": mem or floor_mem}
        say(f"  smallest one-pass budget: {hi} MiB (bisection to 10%, probes {probes})")


# ---------------------------------------------------------------- ledger

LEDGER_RECEIPTS = 400
LEDGER_FILES = 10_000
LEDGER_REPS = 3


def ledger_10k(p):
    """A ledger of at least LEDGER_FILES files holding LEDGER_RECEIPTS
    receipts, each with its certificate signed, the shape PR #10's review
    timed (10,413 files, 401 receipts). Every record is written by the
    binary: per receipt, a bloom key proposal at its own false-positive
    rate and a `fix --signing-key` that scores, verifies, applies, and signs
    it (about ten files each, signatures and store lock files included);
    then key proposals by distinct proposers up to the file count. Kept
    under <work>/ledger-10k and reused while ready.json is there."""
    root = os.path.join(p.a.work, "ledger-10k")
    ready = os.path.join(root, "ready.json")
    if os.path.exists(ready):
        return json.load(open(ready))
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root)
    corpus = os.path.join(root, "corpus")
    gen = ["--days", "1", "--files", "2", "--row-groups-per-file", "4", "--rows-per-row-group", "500",
           "--entities", "1000"]
    sh([p.a.bin, "bench", "generate", "--out", corpus, *gen])
    data = os.path.realpath(os.path.join(corpus, "data"))
    src = ["--partitioning", "hive"]
    keys = os.path.join(root, "keys.parquet")
    duck(f"COPY (SELECT DISTINCT entity_id FROM read_parquet('{data}/*/*.parquet') ORDER BY entity_id "
         f"LIMIT 20) TO '{keys}' (FORMAT PARQUET)")
    why = json.loads(sh([p.a.bin, "why", data, *src, "--from", keys, "--json"])[1])
    evidence, workload = why["evidence"]["hash"], why["workload"]["hash"]
    key, trust = os.path.join(root, "signer"), os.path.join(root, "trust.toml")
    sh([p.a.bin, "keys", "generate", "--out", key, "--name", "perf"])
    sh([p.a.bin, "keys", "trust", "--file", trust, "--add", key + ".pub"])
    say(f"ledger: writing {LEDGER_RECEIPTS} signed receipts")
    for i in range(LEDGER_RECEIPTS):
        spec = f"bloom:entity_id@{0.01 + i * 0.0005:.4f}"
        out = sh([p.a.bin, "propose", data, *src, "--evidence", evidence, "--key", spec, "--json"])[1]
        proposal = json.loads(out)["proposal"]["hash"]
        out = sh([p.a.bin, "fix", data, *src, "--proposal", proposal, "--workload", workload,
                  "--allow-no-gain", "--signing-key", key, "--json"])[1]
        if '"receipt"' not in out:
            sys.exit(f"ledger: fix of {spec} wrote no receipt: {out[:400]}")
    ledger = os.path.join(data, ".bellwether", "ledger")

    def files():
        return sum(len(fs) for _, _, fs in os.walk(ledger))

    say(f"ledger: {files()} files; padding with key proposals to {LEDGER_FILES}")
    from concurrent.futures import ThreadPoolExecutor
    n = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        while files() < LEDGER_FILES:
            batch = range(n, n + max(LEDGER_FILES - files(), 1))
            list(pool.map(lambda j: sh([p.a.bin, "propose", data, *src, "--evidence", evidence, "--key",
                                        "exact:entity_id", "--by", f"perf-pad-{j}", "-q"]), batch))
            n = batch.stop
    info = {"data": data, "trust": trust, "files": files(), "receipts": LEDGER_RECEIPTS,
            "command": "scripts/perf.py ledger_10k: bellwether bench generate --out corpus " + " ".join(gen)
            + f"; {LEDGER_RECEIPTS} x (propose --key bloom:entity_id@<fpr>, fix --allow-no-gain "
            "--signing-key); propose --key exact:entity_id --by perf-pad-<n> to the file count"}
    json.dump(info, open(ready, "w"))
    return info


def section_ledger(p):
    """`ledger receipts` (without and with a trust file) and `ledger
    scorecard` over the 10k-file ledger, warm."""
    lg = ledger_10k(p)
    p.context["ledger/ledger-10k"] = {k: lg[k] for k in ("files", "receipts", "command")}
    for name, extra in (("receipts", []), ("receipts-trusted", ["--trusted-keys", lg["trust"]]),
                        ("scorecard", [])):
        verb = "scorecard" if name == "scorecard" else "receipts"
        cmd = [p.a.bin, "ledger", verb, lg["data"], "--partitioning", "hive", *extra, "--json"]
        _, out, _, _ = sh(cmd)
        if verb == "receipts":
            shown = out.count('"receipt":')
            if shown < lg["receipts"]:
                sys.exit(f"ledger {verb}: {shown} receipts shown, {lg['receipts']} written")
        xs = [sh(cmd)[0] for _ in range(LEDGER_REPS)]
        p.quantiles(f"ledger/ledger-10k/{name}/warm", xs, dataset="ledger-10k",
                    query=f"ledger {verb}{' --trusted-keys' if extra else ''}", metric="ledger",
                    cache="warm", files=lg["files"], receipts=lg["receipts"],
                    command="bellwether " + " ".join(c if c != lg["data"] else "<ledger-10k>"
                                                     for c in cmd[1:]).replace(lg["trust"], "trust.toml"))


# ---------------------------------------------------------------- latency


def cargo_test(p, args, env):
    cmd = ["cargo", "test", "--release", *args, "--", "--nocapture", "--test-threads=1"]
    e = dict(os.environ, **env)
    _, out, err, _ = sh(cmd, env=e, cwd=REPO)
    return out + err


def section_latency(p, replay_small):
    if replay_small:
        corpus = replay_small.root
        say("latency: replay p50 run (bellwether-replay tests/latency.rs)")
        text = cargo_test(p, ["-p", "bellwether-replay", "--test", "latency"],
                          {"BELLWETHER_REPLAY_LATENCY_CORPUS": corpus})
        for m in re.finditer(r"^(.*?): p50 run \((\d+) keys\) replays in ([\d.]+) ms", text, re.M):
            what = m.group(1).strip()
            slug = re.sub(r"[^a-z0-9]+", "-", what.lower()).strip("-")
            p.metric(f"replay/{slug}/p50-run", float(m.group(3)), "ms", "lower", stat="p50",
                     dataset="replay-small" if "bench corpus" in what else "in-test corpus",
                     query=what, metric="replay p50 run", keys=int(m.group(2)), slo_ms=5,
                     command="cargo test --release -p bellwether-replay --test latency")
        p.context["replay/latency-test-output"] = [l for l in text.splitlines() if "replays in" in l or "built" in l]
        say("latency: observe tier (bellwether tests/latency.rs, bellwether-mcp tests/latency.rs)")
        text = cargo_test(p, ["-p", "bellwether", "--test", "latency"],
                          {"BELLWETHER_OBSERVE_LATENCY_CORPUS": corpus})
        m = re.search(r"why \(no workload\): p50 (\S+), all \[(.*?)\]", text)
        if m:
            p.metric("observe/why-no-workload/p50", round(parse_duration(m.group(1)), 3), "ms", "lower",
                     stat="p50", dataset="replay-small", query="why (no workload)", metric="observe",
                     slo_ms=100, n=7, command="cargo test --release -p bellwether --test latency")
        text = cargo_test(p, ["-p", "bellwether-mcp", "--test", "latency"],
                          {"BELLWETHER_OBSERVE_LATENCY_CORPUS": corpus})
        m = re.search(r"observe\.why \(no keys\): p50 in process (\S+), over stdio (\S+)", text)
        if m:
            for side, val in (("in-process", m.group(1)), ("stdio", m.group(2))):
                p.metric(f"mcp/observe.why/{side}/p50", round(parse_duration(val.rstrip(",")), 3), "ms",
                         "lower", stat="p50", dataset="replay-small", query=f"observe.why {side}",
                         metric="observe", slo_ms=100, n=7,
                         command="cargo test --release -p bellwether-mcp --test latency")
        # The observe tier through the CLI, as a user runs it.
        idx = os.path.join(p.a.work, "observe-idx")
        shutil.rmtree(idx, ignore_errors=True)
        cmd = [p.a.bin, "why", replay_small.data, "--index", idx, "--json"]
        sh(cmd)
        xs = [sh(cmd)[0] for _ in range(p.a.warm)]
        p.quantiles("observe/cli-why-no-workload/warm", xs, dataset="replay-small",
                    query="why (no workload)", metric="observe", cache="warm", slo_ms=100,
                    command="bellwether " + " ".join(cmd[1:]))
    say("latency: resident bytes per key (bellwether-replay tests/resident.rs)")
    for test in ("open_writer_resident_bytes_per_key", "layout_map_resident_bytes_per_key"):
        cmd = ["cargo", "test", "--release", "-p", "bellwether-replay", "--test", "resident", "--",
               "--ignored", "--exact", test, "--nocapture"]
        _, out, err, _ = sh(cmd, cwd=REPO)
        m = re.search(r"RESIDENT (\{.*\})", out + err)
        v = json.loads(m.group(1))
        p.metric(f"resident/{v['case']}/bytes-per-key", v["resident_bytes_per_key"], "bytes", "lower",
                 metric="resident bytes per key", query=v["case"], keys=v["keys"],
                 command=" ".join(cmd))
        p.context[f"resident/{v['case']}"] = v


def section_why(p, ledger):
    if not ledger:
        say("why: no small rung (--ci); nothing to measure")
        return
    say("why: end to end on the pre-registered workload (small rung)")
    idx = os.path.join(p.a.work, "why-idx")
    cmd = [p.a.bin, "why", ledger.data, "--bench-workload", ledger.root, "--index", idx, "--json"]
    printable = "bellwether " + " ".join(cmd[1:])
    cold, warm = [], []
    shutil.rmtree(idx, ignore_errors=True)
    for _ in range(p.a.why_reps):
        shutil.rmtree(os.path.join(idx, "cache"), ignore_errors=True)
        p.cache.drop()
        cold.append(sh(cmd)[0])
    for _ in range(p.a.why_reps):
        warm.append(sh(cmd)[0])
    for cache, xs in (("cold", cold), ("warm", warm)):
        p.quantiles(f"why/{ledger.name}/preregistered/{cache}", xs, dataset=ledger.name,
                    query="why --bench-workload (200 runs)", metric="end-to-end", cache=cache,
                    command=printable)


# --------------------------------------------------------------- profiles

IDLE = ("__psynch_cvwait", "__semwait_signal", "mach_msg2_trap", "__workq_kernreturn", "kevent",
        "__psynch_mutexwait", "__ulock_wait", "__ulock_wait2", "__select", "__read_nocancel_wait")


def demangle(sym):
    if sym.startswith("__ZN"):
        sym = sym[1:]
    if not sym.startswith("_ZN"):
        return sym
    s, parts, i = sym[3:], [], 0
    while i < len(s) and s[i] != "E":
        m = re.match(r"\d+", s[i:])
        if not m:
            break
        n = int(m.group())
        i += len(m.group())
        parts.append(s[i:i + n])
        i += n
    if parts and re.fullmatch(r"h[0-9a-f]{16}", parts[-1]):
        parts = parts[:-1]
    out = "::".join(parts)
    for a, b in [("$LT$", "<"), ("$GT$", ">"), ("$u20$", " "), ("$u27$", "'"), ("$u5b$", "["),
                 ("$u5d$", "]"), ("$u7b$", "{"), ("$u7d$", "}"), ("$RF$", "&"), ("$BP$", "*"),
                 ("$C$", ","), ("$SP$", "@"), ("$u3b$", ";"), ("$u2b$", "+"), ("$u22$", '"'),
                 ("..", "::")]:
        out = out.replace(a, b)
    return out


# bellwether's own functions (any workspace crate), by demangled name.
OWN = re.compile(r"(^|[<&\s])bellwether(_[a-z]+)?::")
# The hot paths the WP22 brief expects, each with the frames that make it.
# Every profile reports each one's inclusive share, so a path that does not
# show up is shown as such, not left out.
EXPECTED = {
    "bitmap intersection and union (Planner::intersect, per_key)": r"Planner<.*>>::(intersect|per_key)$",
    "bloom probing (lookup_hashes, bloom::check_bitset)": r"lookup_hashes|bloom::check_bitset",
    "key-directory block decoding (SegmentReader block index, BlockView)": r"BlockView>::find|SegmentReader<.*>>::(block_index|open_for_dataset|open)\b",
    "footer parsing (ParquetSource footer, row_groups)": r"ParquetSource>::footer|ParquetSource as bellwether_types::Source>::row_groups|load_reader_metadata",
    "blake3 over a finished segment (Framing::finish)": r"Framing>::finish",
    "temporal chain compaction (bellwether_core::temporal)": r"bellwether_core::temporal::",
    "reconcile pass hashing (verify rowhash, fold_row)": r"bellwether_verify::(rowhash::|reconcile::fold_row)",
    "store reads per value (SegmentBytes::read_at, get_range)": r"ReadAt>::read_at|StoreExt>::get_range",
}
EXPECTED_RE = {k: re.compile(v) for k, v in EXPECTED.items()}


# A segment value read through the store (one round trip each, hypothesis 1).
READS = re.compile(r"ReadAt>::read_at|StoreExt>::get_range")
GENERIC = re.compile(r"^(bellwether::main|bellwether::run|bellwether::\w+_cmds::run|<?rayon|<?core::|<?std::"
                     r"|<?alloc::)")


def top_frames(profile, syms, n=10):
    """Self time per symbol over every thread, weighted by each sample's CPU
    delta; idle waits are counted separately, not ranked. Also the inclusive
    time of bellwether's own functions, and for the top three self frames the
    nearest bellwether function that called them, so a system frame (an
    `open`, a `malloc`) is tied to the code that caused it."""
    tabs = {}
    for d in syms["data"]:
        st = sorted(d["symbol_table"], key=lambda e: e["rva"])
        tabs[d["code_id"].upper()] = ([e["rva"] for e in st], st)
    names = syms["string_table"]
    libs = profile["libs"]
    idle_names = {x.lstrip("_") for x in IDLE}
    agg, total, idle = {}, 0, 0
    inclusive, leaf_callers = {}, {}
    wall, wall_inclusive = 0, {}
    cpu_total, inclusive_no_reads = 0, {}
    waits, wait_callers = {}, {}
    thread_cpu = []
    expected = {}
    for th in profile["threads"]:
        smp = th["samples"]
        weights = smp.get("threadCPUDelta") or [1] * len(smp["stack"])
        ft, stk, fu, rs = th["frameTable"], th["stackTable"], th["funcTable"], th["resourceTable"]
        cache = {}

        def frame_name(f):
            if f in cache:
                return cache[f]
            addr, func = ft["address"][f], ft["func"][f]
            res = fu["resource"][func]
            lib = libs[rs["lib"][res]] if res is not None and res >= 0 and rs["lib"][res] is not None else None
            name, libname = "?", lib["name"] if lib else "?"
            if lib and addr is not None and addr >= 0:
                tab = tabs.get(lib["codeId"].upper())
                if tab:
                    i = bisect.bisect_right(tab[0], addr) - 1
                    if i >= 0 and addr < tab[1][i]["rva"] + max(tab[1][i]["size"], 1):
                        name = demangle(names[tab[1][i]["symbol"]])
            cache[f] = (name, libname)
            return cache[f]

        def own_frames(s):
            """bellwether's own functions on the stack ending at s, leaf
            first."""
            out = []
            while s is not None:
                nm = frame_name(stk["frame"][s])[0]
                if OWN.search(nm):
                    out.append(nm)
                s = stk["prefix"][s]
            return out

        main = th.get("isMainThread")
        thread_cpu.append(sum(w for w in weights if w))
        prev = None
        for s, w in zip(smp["stack"], weights):
            if s is None:
                continue
            if main:
                # Wall time on the main thread: every sample counts, on CPU
                # or waiting, so a wait shows up under the code that waits.
                wall += 1
                for nm in set(own_frames(s)):
                    wall_inclusive[nm] = wall_inclusive.get(nm, 0) + 1
            # A sample's CPU delta is the CPU the thread used since the
            # previous sample; a thread found waiting now was running then,
            # so the delta is credited to the previous sample's stack.
            at, prev = (prev if prev is not None else s), s
            if not w:
                continue
            cpu_total += w
            name, libname = frame_name(stk["frame"][at])
            key = f"{name} [{libname}]"
            if name.lstrip("_") in idle_names:
                # CPU charged to a wait primitive is kernel time sleeping and
                # waking the thread; name the code that waited.
                idle += w
                waits[key] = waits.get(key, 0) + w
                q = stk["prefix"][at]
                while q is not None:
                    nm, lib = frame_name(stk["frame"][q])
                    if lib == "bellwether" and nm != "?" and not GENERIC.search(nm):
                        wc = wait_callers.setdefault(key, {})
                        wc[nm] = wc.get(nm, 0) + w
                        break
                    q = stk["prefix"][q]
                continue
            agg[key] = agg.get(key, 0) + w
            total += w
            mine = own_frames(at)
            reads = any(READS.search(nm) for nm in mine)
            for path, rx in EXPECTED_RE.items():
                if any(rx.search(nm) for nm in mine):
                    expected[path] = expected.get(path, 0) + w
            for nm in set(mine):
                inclusive[nm] = inclusive.get(nm, 0) + w
                if not reads:
                    inclusive_no_reads[nm] = inclusive_no_reads.get(nm, 0) + w
            if not OWN.search(name):
                # The nearest bellwether function, or on a thread with none
                # (a blocking-I/O pool), the nearest named library function.
                caller = mine[0] if mine else None
                q = stk["prefix"][at]
                while caller is None and q is not None:
                    nm, lib = frame_name(stk["frame"][q])
                    if lib == "bellwether" and nm != "?" and not GENERIC.search(nm) and not nm.startswith(("tokio::", "<tokio::")):
                        caller = nm
                    q = stk["prefix"][q]
                if caller:
                    c = leaf_callers.setdefault(key, {})
                    c[caller] = c.get(caller, 0) + w
    top = sorted(agg.items(), key=lambda kv: -kv[1])[:n]
    pct = lambda v: round(100 * v / max(1, total), 1)

    def ranked(d):
        # Entry points and thread-pool plumbing are on every stack; leave
        # them out so the table shows where the time goes.
        keep = [(k, v) for k, v in d.items() if not GENERIC.search(k)]
        return sorted(keep, key=lambda kv: -kv[1])[:15]
    unit = (profile.get("meta", {}).get("sampleUnits") or {}).get("threadCPUDelta", "µs")
    per_s = {"ns": 1e9, "µs": 1e6, "us": 1e6, "ms": 1e3}.get(unit)
    return {"cpu_weight": cpu_total, "on_cpu_weight": total, "idle_weight": idle,
            "cpu_seconds": round(cpu_total / per_s, 2) if per_s else None,
            "top": [{"frame": k, "self_pct": pct(v)} for k, v in top],
            "inclusive": [{"frame": k, "total_pct": pct(v),
                           "without_store_reads_pct": pct(inclusive_no_reads.get(k, 0))}
                          for k, v in ranked(inclusive)],
            "callers": {k: [{"frame": c, "pct": pct(v)} for c, v in
                            sorted(leaf_callers.get(k, {}).items(), key=lambda kv: -kv[1])[:3]]
                        for k, _ in top[:3]},
            "waits": [{"frame": k, "cpu_pct": round(100 * v / max(1, cpu_total), 1),
                       "callers": [{"frame": c, "cpu_pct": round(100 * x / max(1, cpu_total), 1)} for c, x in
                                   sorted(wait_callers.get(k, {}).items(), key=lambda kv: -kv[1])[:2]]}
                      for k, v in sorted(waits.items(), key=lambda kv: -kv[1])[:4]],
            # How the CPU spread over threads: one busy thread among many
            # idle ones is a parallelism problem, not a per-row cost.
            "threads": len(thread_cpu),
            "busiest_threads_cpu_pct": [round(100 * x / max(1, cpu_total), 1)
                                        for x in sorted(thread_cpu, reverse=True)[:3]],
            "expected_paths": {k: pct(expected.get(k, 0)) for k in EXPECTED},
            "main_thread_wall_samples": wall,
            "main_thread_wall": [{"frame": k, "wall_pct": round(100 * v / max(1, wall), 1)}
                                 for k, v in ranked(wall_inclusive)]}


def section_profiles(p, mkt10, ledger, mdef=None):
    samply = shutil.which("samply")
    if not samply:
        say("skipping profiles: samply is not on PATH (cargo install samply)")
        return
    outdir = os.path.join(p.a.out, "profiles")
    os.makedirs(outdir, exist_ok=True)
    bw = p.a.bin
    jobs = []
    if mdef:
        # The plan floor: the fixed cost of one small lookup process (start,
        # manifest, staleness check, the durable Evidence write).
        ensure_index(p, mdef)
        jobs.append(("plan-floor-market-default-10", 200, [bw, "latest", mdef.data, "--keys",
                     key_file(p, mdef, 10), "--index", mdef.idx, "--quiet"], None))
    if mkt10:
        ensure_index(p, mkt10)
        at = mid_hour(mkt10.span)
        k3000 = key_file(p, mkt10, 3000)
        k300 = key_file(p, mkt10, 300)
        jobs.append(("window-3000", 20, [bw, "locate", mkt10.data, "--keys", k3000, "--window",
                     f"{iso_us(at - US_PER_DAY)}..{iso_us(at)}", "--ts", "ts", "--index", mkt10.idx,
                     "--quiet"], None))
        jobs.append(("latest-300", 30, [bw, "latest", mkt10.data, "--keys", k300, "--index", mkt10.idx,
                     "--quiet"], None))
        bidx = os.path.join(p.a.work, "profile-build-idx")
        jobs.append(("build-market-10x-all-threads", 1, [bw, *mkt10.index_args(idx=bidx), "--quiet"], bidx))
    if ledger:
        four = p.context.get("profile/four-pass-budget")
        if not four:
            # The budget that makes four passes: a quarter of the one-pass need.
            one = p.context.get(f"reconcile/{ledger.name}/smallest-one-pass", {})
            mib = max(1, one.get("budget_mib", 1024) // 4) if isinstance(one, dict) else 256
            four = f"{mib}M"
        right = ledger.data + "-copy"
        if os.path.isdir(right):
            jobs.append(("reconcile-four-pass", 1, [bw, "reconcile", ledger.data, right, "--key",
                         "entity_id", "--latest-by", "version_ts", "--totals", "amount", "--group-by",
                         "status", "--memory-budget", four, "--index",
                         os.path.join(p.a.work, "profile-reconcile-idx"), "--quiet"], None))
        lidx = os.path.join(p.a.work, "profile-build-ledger-idx")
        jobs.append(("build-ledger-small-all-threads", 1, [bw, *ledger.index_args(idx=lidx), "--quiet"], lidx))
        widx = os.path.join(p.a.work, "why-idx")
        jobs.append(("why-200-runs", 1, [bw, "why", ledger.data, "--bench-workload", ledger.root,
                     "--index", widx, "--quiet"], None))
    for name, iters, cmd, fresh in jobs:
        if fresh:
            shutil.rmtree(fresh, ignore_errors=True)
        prof = os.path.join(outdir, f"{name}.json.gz")
        say(f"profile: {name}")
        sh([samply, "record", "--save-only", "--unstable-presymbolicate", "--rate", str(p.a.profile_rate),
            "--iteration-count", str(iters), "--reuse-threads", "-o", prof, "--", *cmd])
        with gzip.open(prof) as f:
            profile = json.load(f)
        with open(prof.replace(".json.gz", ".json.syms.json")) as f:
            syms = json.load(f)
        top = top_frames(profile, syms)
        top["command"] = "bellwether " + " ".join(str(x) for x in cmd[1:])
        top["iterations"] = iters
        top["rate_hz"] = p.a.profile_rate
        p.context[f"profile/{name}"] = top
        for i, t in enumerate(top["top"], 1):
            say(f"  {i:2d}. {t['self_pct']:5.1f}%  {t['frame']}")
        # The same command once more without the profiler, under
        # /usr/bin/time: how much CPU the process really used per second of
        # wall time (the profile's CPU weights include the sampler's effect).
        if fresh:
            shutil.rmtree(fresh, ignore_errors=True)
        _, _, err, _ = sh(["/usr/bin/time", "-p", *cmd])
        t = {k: float(v) for k, v in re.findall(r"^(real|user|sys)\s+([\d.]+)", err, re.M)}
        if t.get("real"):
            p.context[f"profile/{name}"]["unprofiled"] = {
                "wall_s": t["real"], "user_s": t.get("user"), "sys_s": t.get("sys"),
                "cores_busy": round((t.get("user", 0) + t.get("sys", 0)) / t["real"], 2)}
        # Read whether the four-pass budget gave four passes.
        if name == "reconcile-four-pass":
            v = json.loads(sh([*cmd[:-1], "--json"])[1])
            p.context[f"profile/{name}"]["passes"] = v["outcome"]["memory"]["passes"]


def reanalyze_profiles(out):
    path = os.path.join(out, "perf.json")
    doc = json.load(open(path))
    for key, old in list(doc["context"].items()):
        if not key.startswith("profile/") or not isinstance(old, dict) or "top" not in old:
            continue
        prof = os.path.join(out, "profiles", key.split("/", 1)[1] + ".json.gz")
        with gzip.open(prof) as f:
            profile = json.load(f)
        with open(prof.replace(".json.gz", ".json.syms.json")) as f:
            syms = json.load(f)
        keep = {k: v for k, v in old.items() if k in ("command", "iterations", "rate_hz", "passes", "unprofiled")}
        doc["context"][key] = {**top_frames(profile, syms), **keep}
        say(f"reanalyzed {key}")
    with open(path, "w") as f:
        json.dump(doc, f, indent=1)
        f.write("\n")


# ------------------------------------------------------------------- main


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bin", help="the release bellwether binary (required unless --reanalyze-profiles)")
    ap.add_argument("--work", help="scratch directory for corpora and indexes (required unless --reanalyze-profiles)")
    ap.add_argument("--out", default=os.path.join(REPO, "benches", "out"))
    ap.add_argument("--realdata", default=os.environ.get("REALDATA_WORK", ""),
                    help="directory holding gh/, taxi/, and taxi_duck/ from scripts/realdata.sh")
    ap.add_argument("--showcase", help="also write the README chart rows here")
    ap.add_argument("--sections", default="lookups,build,reconcile,latency,why,profiles,ledger")
    ap.add_argument("--ci", action="store_true",
                    help="small corpora only (replay-small, the quick corpus, market defaults, the 10k-file "
                         "ledger); no profiles")
    ap.add_argument("--merge", action="store_true",
                    help="keep the other sections' results in an existing <out>/perf.json")
    ap.add_argument("--max-load", type=float, default=3.0,
                    help="warn when processes outside this run use more cores than this during a section")
    ap.add_argument("--reconcile-datasets", default="ledger,replay,taxi",
                    help="which reconcile pairs to run: ledger, replay, taxi")
    ap.add_argument("--reconcile-budgets", default=",".join(l for l, _ in BUDGETS),
                    help="which of the listed budgets to run (the one-pass search always runs)")
    ap.add_argument("--reconcile-cap-s", type=int, default=900,
                    help="skip a budget whose predicted wall time is past this many seconds")
    ap.add_argument("--warm", type=int, default=20)
    ap.add_argument("--cold", type=int, default=5)
    ap.add_argument("--build-reps", type=int, default=3)
    ap.add_argument("--why-reps", type=int, default=3)
    ap.add_argument("--key-sets", default="10,300,3000")
    ap.add_argument("--profile-rate", type=int, default=4000)
    ap.add_argument("--build-label", default="release (thin LTO, codegen-units 1, line-table debug info)")
    ap.add_argument("--built-from",
                    help="the git work tree --bin was built from, when not this one (a before/after "
                         "pair measured with one script)")
    ap.add_argument("--reanalyze-profiles", action="store_true",
                    help="recompute the frame tables of the profiles already saved under <out>/profiles "
                         "into <out>/perf.json, without running anything")
    a = ap.parse_args()
    if a.reanalyze_profiles:
        reanalyze_profiles(a.out)
        return
    if not a.bin or not a.work:
        ap.error("--bin and --work are required")
    a.key_sets = [int(x) for x in a.key_sets.split(",")]
    a.reconcile_datasets = a.reconcile_datasets.split(",")
    a.reconcile_budgets = a.reconcile_budgets.split(",")
    if set(a.reconcile_budgets) - {l for l, _ in BUDGETS}:
        ap.error(f"--reconcile-budgets: choose from {', '.join(l for l, _ in BUDGETS)}")
    a.bin = os.path.realpath(a.bin)
    a.work = os.path.realpath(a.work)
    os.makedirs(a.work, exist_ok=True)
    sections = set(a.sections.split(","))
    if a.ci:
        sections -= {"profiles"}
    unknown = sections - set(SECTION_PREFIXES)
    if unknown:
        sys.exit(f"unknown sections {sorted(unknown)}; choose from {sorted(SECTION_PREFIXES)}")
    a.run_sections = sorted(sections)
    p = Perf(a)
    say(f"machine: {p.env['machine']}\nbuild: {p.env['build']}, {p.env['rustc']}, commit "
        f"{p.env['commit']}{' (dirty)' if p.env['commit_dirty'] else ''}\ncold: {p.cache.label}")

    # Corpora only for the sections that read them.
    want = lambda *names: bool(sections & set(names))  # noqa: E731
    mdef = market_dataset(p, "market-default", []) if want("lookups", "build", "profiles") else None
    mkt10 = None if a.ci or not want("lookups", "build", "profiles") else market_dataset(
        p, "market-10x", ["--symbols", "3000", "--days", "30", "--prints-per-symbol-per-day", "12"])
    gh = None if a.ci or not want("lookups") else gh_dataset(p)
    replay_small = None if not (want("latency") or (a.ci and want("build", "reconcile"))) else ledger_dataset(
        p, "replay-small", ["--days", "4", "--files", "64", "--row-groups-per-file", "2",
                            "--rows-per-row-group", "2000", "--entities", "640000", "--churn", "0.1"])
    ledger = None if a.ci or not want("build", "reconcile", "why", "profiles") else ledger_dataset(
        p, "ledger-small", ["--rung", "small"])

    runs = [
        ("lookups", lambda: section_lookups(p, [d for d in (mdef, mkt10, gh) if d])),
        ("build", lambda: section_build(p, [d for d in (replay_small if a.ci else ledger, mdef, mkt10) if d])),
        ("reconcile", lambda: section_reconcile(p, replay_small if a.ci else ledger)),
        ("latency", lambda: section_latency(p, replay_small)),
        ("why", lambda: section_why(p, ledger)),
        ("profiles", lambda: section_profiles(p, mkt10, ledger, mdef)),
        ("ledger", lambda: section_ledger(p)),
    ]
    for name, run in runs:
        if name in sections:
            # Reconcile pairs may be measured in separate slots (--merge), so
            # each pair set keeps its own section record.
            key = name
            if name == "reconcile":
                key = f"reconcile {','.join(a.reconcile_datasets)}"
                if len(a.reconcile_budgets) < len(BUDGETS):
                    key += f" at {','.join(a.reconcile_budgets)}"
            if name == "reconcile":
                # A rerun supersedes older records of the same pairs and
                # budgets.
                for old in [k for k in p.sections if k.startswith("reconcile ") and k != key]:
                    w = old.split(" ")
                    budgets = w[3].split(",") if len(w) == 4 else [l for l, _ in BUDGETS]
                    if set(w[1].split(",")) <= set(a.reconcile_datasets) and set(budgets) <= set(a.reconcile_budgets):
                        del p.sections[old]
            p.section(key, start=True)
            run()
            p.section(key, start=False)
            p.write()


if __name__ == "__main__":
    main()
