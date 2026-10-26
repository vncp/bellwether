#!/usr/bin/env python3
"""Markdown tables from a perf.json (bellwether-perf/1), for the measured
document. Prints to stdout; nothing in the tables is typed by hand.

  scripts/perf-report.py benches/out/perf.json [--section lookups|build|reconcile|latency|profiles]
"""

import argparse
import json


def fmt_ms(v):
    if v is None:
        return "-"
    return (f"{v:,.0f}" if v >= 100 else f"{v:,.1f}" if v >= 10 else f"{v:,.2f}" if v >= 0.1
            else f"{v:,.3f}")


def mib(v):
    return f"{v / 2**20:,.1f}"


class Doc:
    def __init__(self, path):
        self.d = json.load(open(path))
        self.m = {x["id"]: x for x in self.d["metrics"]}
        self.c = self.d["context"]

    def v(self, mid):
        x = self.m.get(mid)
        return x["value"] if x else None

    def runs(self):
        """When each section ran, at which commit, and how busy the machine
        was outside the run (the load average also counts the run itself)."""
        out = ["| section | started (UTC) | finished | commit | 1-min load: start / max | cores used outside the run, max | busiest outside process at that peak |",
               "|---|---|---|---|---:|---:|---|"]
        for name, s in self.d["environment"].get("sections", {}).items():
            top = (s.get("external_top_at_max") or ["-"])[0]
            top = top.rsplit("/", 1)[-1]
            out.append(f"| {name} | {s['started_at']} | {s.get('finished_at', '-')} | `{s['commit']}`"
                       f"{' (dirty)' if s.get('commit_dirty') else ''} | {s['load_avg_1m_start']} / "
                       f"{s.get('load_avg_1m_max', '-')} | {s.get('external_cpu_cores_max', '-')} | {top} |")
        return "\n".join(out)

    def lookups(self):
        out = []
        datasets = [k.split("/", 1)[1] for k in self.c if k.startswith("dataset/")]
        for ds in datasets:
            ctx = self.c[f"dataset/{ds}"]
            fs = ctx["full_scan"]
            out.append(f"#### {ds}\n")
            out.append(f"`{ctx['command']}`: {fs['files']} files, {fs['row_groups']:,} row groups, "
                       f"{fs['rows']:,} rows, {fs['bytes_fetched']:,} column-chunk bytes; time column span "
                       f"{ctx['span'][0]} to {ctx['span'][1]}; {ctx['distinct_keys']:,} distinct keys; "
                       f"specs `{'`, `'.join(ctx['specs'])}`.\n")
            for cache in ("warm", "cold"):
                fsp = (self.v(f"lookup/{ds}/full-scan/{cache}/p50"), self.v(f"lookup/{ds}/full-scan/{cache}/p95"))
                out.append(f"{cache.capitalize()} (ms, p50 / p95; full scan of every row group: "
                           f"{fmt_ms(fsp[0])} / {fmt_ms(fsp[1])}):\n")
                out.append("| lookup | keys | planned row groups | planned bytes | plan | plan + read | DuckDB |")
                out.append("|---|---:|---:|---:|---:|---:|---:|")
                keys = sorted({int(k.split("/")[3][1:]) for k in self.c
                               if k.startswith(f"lookup/{ds}/") and len(k.split("/")) == 4})
                names = []
                for k in self.c:
                    p = k.split("/")
                    if k.startswith(f"lookup/{ds}/") and len(p) == 4 and p[2] not in names:
                        names.append(p[2])
                for k in keys:
                    for name in names:
                        base = f"lookup/{ds}/{name}/k{k}"
                        ctxl = self.c.get(base)
                        if not ctxl:
                            continue
                        cell = lambda what: (f"{fmt_ms(self.v(f'{base}/{what}/{cache}/p50'))} / "
                                             f"{fmt_ms(self.v(f'{base}/{what}/{cache}/p95'))}")
                        out.append(f"| `{name}` | {k:,} | {ctxl['planned']['row_groups']:,} of "
                                   f"{ctxl['row_groups_total']:,} | {ctxl['planned']['bytes']:,} | "
                                   f"{cell('plan')} | {cell('e2e')} | {cell('duckdb')} |")
                out.append("")
        skipped = self.c.get("skipped")
        if skipped:
            out.append("Not run: " + "; ".join(skipped) + ".\n")
        return "\n".join(out)

    def build(self):
        out = []
        for k, ctx in self.c.items():
            if not k.startswith("build/"):
                continue
            ds = k.split("/")[1]
            base = self.v(f"build/{ds}/noop-peak-rss")
            out.append(f"#### {ds}: {ctx['row_groups']:,} row groups, specs `{'`, `'.join(ctx['specs'])}`\n")
            out.append(f"Peak RSS of the same command over a complete index (nothing to build): {mib(base)} MiB.\n")
            out.append("| `--threads` | wall p50 (ms) | row groups / s | speedup | peak RSS (MiB) | per worker over the no-op (MiB) | segments |")
            out.append("|---|---:|---:|---:|---:|---:|---:|")
            one = self.v(f"build/{ds}/t1/row-groups-per-s")
            for t in ("1", "2", "4", "8", "all"):
                r = self.v(f"build/{ds}/t{t}/row-groups-per-s")
                if r is None:
                    continue
                out.append(f"| {t} | {fmt_ms(self.v(f'build/{ds}/t{t}/wall/p50'))} | {r:,.0f} | "
                           f"{r / one:.2f}x | {mib(self.v(f'build/{ds}/t{t}/peak-rss'))} | "
                           f"{mib(self.v(f'build/{ds}/t{t}/per-worker-rss'))} | "
                           f"{self.v(f'build/{ds}/t{t}/segments')} |")
            out.append("")
        return "\n".join(out)

    def reconcile(self):
        out = []
        names = []
        for k in self.c:
            p = k.split("/")
            if p[0] == "reconcile" and p[1] not in names:
                names.append(p[1])
        for ds in names:
            first = next(v for k, v in self.c.items() if k.startswith(f"reconcile/{ds}/") and isinstance(v, dict) and "memory" in v)
            out.append(f"#### {ds}: {first['rows'][0]:,} rows left, {first['rows'][1]:,} right ({first['what']}); "
                       f"pass: {first['pass']}\n")
            out.append("| budget | wall (s) | rows / s per side | passes | meter peak (MiB) | peak RSS (MiB) |")
            out.append("|---|---:|---:|---:|---:|---:|")
            skipped = []
            for b in ("64M", "256M", "1G", "8G"):
                w = self.v(f"reconcile/{ds}/{b}/wall")
                if w is None:
                    nr = self.c.get(f"reconcile/{ds}/{b}", {}).get("not_run")
                    if nr:
                        skipped.append(f"{b}: not run, {nr}")
                    continue
                out.append(f"| {b} | {w / 1000:,.1f} | {self.v(f'reconcile/{ds}/{b}/rows-per-s-per-side'):,} | "
                           f"{self.v(f'reconcile/{ds}/{b}/passes')} | {mib(self.v(f'reconcile/{ds}/{b}/meter-peak'))} | "
                           f"{mib(self.v(f'reconcile/{ds}/{b}/peak-rss'))} |")
            if skipped:
                out.append("\n" + "; ".join(skipped) + ".")
            one = self.c.get(f"reconcile/{ds}/smallest-one-pass")
            if isinstance(one, dict) and one.get("above"):
                out.append("\nNo listed budget gave one pass, so the search doubled past 8 GiB: "
                           + "; ".join(f"{a['budget_mib']:,} MiB: {a['passes']} pass(es), meter peak "
                                       f"{mib(a['meter_peak'])} MiB, peak RSS {mib(a['peak_rss'])} MiB, "
                                       f"{a['wall_ms'] / 1000:,.1f} s" for a in one["above"]) + ".")
            if isinstance(one, dict) and "none" in one:
                out.append(f"\nSmallest budget with one pass: none found ({one['none']}).")
            elif isinstance(one, dict):
                mem = one["memory_at_budget"]
                out.append(f"\nSmallest budget with one pass (bisection to 10%): **{one['budget_mib']:,} MiB**; the "
                           f"meter's peak at that budget was {mib(mem['peak_state_bytes'])} MiB, so one pass "
                           f"needs {one['budget_mib'] / (mem['peak_state_bytes'] / 2**20):.2f}x the state it "
                           f"then holds. Probes (MiB, passes, meter peak MiB): "
                           + (", ".join(f"{m:,} {p} {mib(s)}" for m, p, s in one["probes"]) or "none needed") + ".")
            elif one:
                out.append(f"\nSmallest budget with one pass: {one}.")
            out.append("")
        return "\n".join(out)

    def latency(self):
        out = ["| measurement | corpus | value | budget |", "|---|---|---:|---:|"]
        for mid, m in self.m.items():
            if mid.startswith(("replay/", "observe/", "mcp/", "why/", "resident/")) and m["direction"] != "none":
                if mid.endswith("/p95") and not mid.startswith("why/"):
                    continue
                unit = m["unit"]
                val = f"{fmt_ms(m['value'])} ms" if unit == "ms" else f"{m['value']:,} {unit}"
                slo = f"{m['slo_ms']} ms" if m.get("slo_ms") else "-"
                out.append(f"| `{mid}` | {m.get('dataset') or '-'} | {val} | {slo} |")
        return "\n".join(out)

    def profiles(self):
        out = []
        for k, v in self.c.items():
            if not k.startswith("profile/") or not isinstance(v, dict) or "top" not in v:
                continue
            name = k.split("/", 1)[1]
            idle = v["idle_weight"] / max(1, v["idle_weight"] + v["on_cpu_weight"]) * 100
            out.append(f"#### {name}\n")
            out.append(f"`{v['command']}`, {v['iterations']} iteration(s) at {v['rate_hz']} Hz"
                       + (f", {v['passes']} passes" if "passes" in v else "")
                       + f"; idle waits are {idle:.0f}% of sampled weight and are not ranked.\n")
            out.append("| # | self % (on CPU) | frame |")
            out.append("|---:|---:|---|")
            for i, t in enumerate(v["top"], 1):
                frame = t["frame"].replace("|", "\\|")
                out.append(f"| {i} | {t['self_pct']} | `{frame}` |")
            callers = v.get("callers") or {}
            if any(callers.values()):
                out.append("\nWho called the top self frames that are not bellwether's own: the nearest "
                           "bellwether function, or on a thread with none (a blocking-I/O pool) the nearest "
                           "named library function (% of on-CPU time):\n")
                for frame, cs in callers.items():
                    if cs:
                        out.append(f"- `{frame}`: " + "; ".join(f"`{c['frame']}` {c['pct']}" for c in cs))
            if v.get("inclusive"):
                out.append("\nbellwether functions by inclusive on-CPU time (every thread):\n")
                out.append("| # | total % (on CPU) | function |")
                out.append("|---:|---:|---|")
                for i, t in enumerate(v["inclusive"], 1):
                    out.append(f"| {i} | {t['total_pct']} | `{t['frame']}` |")
            if v.get("main_thread_wall"):
                out.append(f"\nMain thread wall time ({v['main_thread_wall_samples']:,} samples, on CPU or "
                           "waiting), by bellwether function on the stack:\n")
                out.append("| # | wall % (main thread) | function |")
                out.append("|---:|---:|---|")
                for i, t in enumerate(v["main_thread_wall"], 1):
                    out.append(f"| {i} | {t['wall_pct']} | `{t['frame']}` |")
            out.append("")
        return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("perf")
    ap.add_argument("--section", default="all")
    a = ap.parse_args()
    d = Doc(a.perf)
    for s in ("runs", "lookups", "build", "reconcile", "latency", "profiles"):
        if a.section in ("all", s):
            print(f"<!-- generated by scripts/perf-report.py --section {s} -->\n")
            print(getattr(d, s)())


if __name__ == "__main__":
    main()
