#!/usr/bin/env python3
"""Writes docs/measured/budgets.toml from a perf.json (bellwether-perf/1).

  scripts/perf-budgets.py --perf benches/out/perf.json \
      --document docs/measured/2026-10-26-performance.md > docs/measured/budgets.toml

Every metric with a direction gets a budget: a latency (direction "lower")
gets measured x 1.25, a throughput (direction "higher") measured / 1.25.
Context metrics (direction "none": counts, passes, bytes planned), DuckDB's
times (the baseline), and reconcile meter peaks (bounded by the budget by
construction) get none. A reconcile peak-memory budget is never above the
verify README's stated bound (ADR 018): where the measurement is past it, the
budget is the bound, so the check fails until the defect is fixed.
The file names the measurement it derives from (date, commit, machine,
build). With --runner-perf, the [runner] section holds the CI runner's own
baseline from a perf.yml baseline run, with its commit; without it,
[runner] says it is not measured yet. `bellwether bench perf-check` reads
this file (perf.yml with --runner).
"""

import argparse
import json
import sys

FACTOR = 1.25

# Which section of scripts/perf.py measured a metric, by id prefix.
SECTION = {"lookup": "lookups", "process": "lookups", "build": "build", "reconcile": "reconcile",
           "replay": "latency", "observe": "latency", "mcp": "latency", "why": "why",
           "resident": "latency", "profile": "profiles", "ledger": "ledger"}


def section_of(mid, sections):
    """The section record (when, at which commit, under what load) behind a
    metric; reconcile pairs measured in separate slots have their own."""
    parts = mid.split("/")
    name = SECTION.get(parts[0])
    if name == "reconcile":
        # Keys look like "reconcile ledger,replay" or "reconcile taxi at 8G".
        short = parts[1].split("-")[0]
        whole = None
        for key, rec in sections.items():
            words = key.split(" ")
            if words[0] != "reconcile" or short not in words[1].split(","):
                continue
            if len(words) == 4 and parts[2] in words[3].split(","):
                return rec
            if len(words) == 2:
                whole = rec
        return whole
    return sections.get(name)


FOOTER_CACHE = 256 << 20


def stated_bound(m, context):
    """A reconcile peak's ADR 018 bound (the verify README): the budget, one
    decoded batch, and the footer cache; from the metric or its context."""
    if "bound_bytes" in m:
        return m["bound_bytes"]
    parts = m["id"].split("/")
    if parts[0] == "reconcile" and len(parts) == 4 and parts[3] in ("peak-rss", "peak-footprint"):
        mem = context.get(f"reconcile/{parts[1]}/{parts[2]}", {}).get("memory")
        if mem:
            return mem["budget_bytes"] + mem["peak_batch_bytes"] + FOOTER_CACHE
    return None


def q(s):
    return json.dumps(s, ensure_ascii=False)


def budget_lines(doc, table):
    """`[[<table>]]` entries for every budgeted metric of a perf.json."""
    env = doc["environment"]
    out, n = [], 0
    for m in doc["metrics"]:
        if m["direction"] not in ("lower", "higher"):
            continue
        v = m["value"]
        if v is None:
            continue
        if m.get("side") == "duckdb" or m.get("metric") == "meter peak state":
            # The baseline and the meter (bounded by the budget by
            # construction) are context, never budgets.
            continue
        budget = v * FACTOR if m["direction"] == "lower" else v / FACTOR
        bound = stated_bound(m, doc["context"])
        capped = bound is not None and budget > bound
        if capped:
            budget = bound
        sec = section_of(m["id"], env.get("sections", {}))
        out += [
            f"[[{table}]]",
            f"id = {q(m['id'])}",
            f"unit = {q(m['unit'])}",
            f"direction = {q(m['direction'])}",
            f"measured = {round(v, 3)}",
            f"budget = {round(budget, 3)}",
        ]
        if bound is not None:
            out.append(f"stated_bound = {bound}")
        if capped:
            # Measured past the verify README's stated bound: the budget is
            # the bound, so the check fails until the defect is fixed.
            out.append("capped_at_stated_bound = true")
        if sec:
            out += [f"measured_at = {q(sec['started_at'])}", f"commit = {q(sec['commit'])}",
                    # Cores busy outside the run during its section: a loaded
                    # section's budgets are loose by that much.
                    f"outside_cores_max = {sec.get('external_cpu_cores_max', 0)}"]
        out.append("")
        n += 1
    return out, n


def load(path):
    doc = json.load(open(path))
    if doc.get("format") != "bellwether-perf/1":
        sys.exit(f"{path} is not bellwether-perf/1")
    return doc


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--perf", required=True)
    ap.add_argument("--document", required=True)
    ap.add_argument("--runner-perf",
                    help="the perf.json of a perf.yml baseline run (workflow_dispatch with baseline), "
                         "committed beside the document: written as the [runner] budgets")
    a = ap.parse_args()
    doc = load(a.perf)
    env = doc["environment"]
    out = [
        "# Performance budgets (WP22). Generated by scripts/perf-budgets.py from the",
        "# perf.json behind the document below; re-baseline only by a pull request that",
        "# changes this file together with the measured document that justifies it.",
        "# `bellwether bench perf-check` asserts them (perf.yml asserts [runner]).",
        'format = "bellwether-budgets/1"',
        f"document = {q(a.document)}",
        f"perf = {q(a.perf)}",
        f"date = {q(env['date'])}",
        f"commit = {q(env['commit'])}",
        f"machine = {q(env['machine'])}",
        f"build = {q(env['build'] + ', ' + (env.get('rustc') or ''))}",
        f'rule = "latency: budget = measured x {FACTOR}; throughput: budget = measured / {FACTOR}"',
        "",
    ]
    lines, n = budget_lines(doc, "budget")
    out += lines
    out += [
        "# The CI runner's own baseline: hosted runners are slower and noisier than the",
        "# measurement machine, so perf.yml compares against budgets measured on the",
        "# runner by a baseline run of perf.yml (workflow_dispatch, baseline).",
        "[runner]",
    ]
    if a.runner_perf:
        rdoc = load(a.runner_perf)
        renv = rdoc["environment"]
        out += [f"perf = {q(a.runner_perf)}", f"date = {q(renv['date'])}",
                f"commit = {q(renv['commit'])}", f"machine = {q(renv['machine'])}",
                f"build = {q(renv['build'] + ', ' + (renv.get('rustc') or ''))}", ""]
        lines, r = budget_lines(rdoc, "runner.budget")
        out += lines
        print(f"{r} runner budgets", file=sys.stderr)
    else:
        out += ['status = "not measured yet: run perf.yml with baseline"', ""]
    sys.stdout.write("\n".join(out))
    print(f"{n} budgets", file=sys.stderr)


if __name__ == "__main__":
    main()
