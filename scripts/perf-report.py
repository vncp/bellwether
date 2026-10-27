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
        out = ["| section | started (UTC) | finished | commit | 1-min load: start / max | cores used outside the run: max / median | samples over 2 cores | busiest outside process at the peak |",
               "|---|---|---|---|---:|---:|---:|---|"]
        for name, s in self.d["environment"].get("sections", {}).items():
            top = (s.get("external_top_at_max") or ["-"])[0]
            top = top.rsplit("/", 1)[-1]
            out.append(f"| {name} | {s['started_at']} | {s.get('finished_at', '-')} | `{s['commit']}`"
                       f"{' (dirty)' if s.get('commit_dirty') else ''} | {s['load_avg_1m_start']} / "
                       f"{s.get('load_avg_1m_max', '-')} | {s.get('external_cpu_cores_max', '-')} / "
                       f"{s.get('external_cpu_cores_p50', '-')} | "
                       + (f"{s['external_cpu_share_over_2_cores'] * 100:.0f}% of {s['external_cpu_samples']}"
                          if "external_cpu_samples" in s else "not recorded") + f" | {top} |")
        prev = self.c.get("lookups-previous-attempt")
        if prev:
            s = prev["section"]
            ratios = sorted(self.v(k) / v for k, v in prev["plan_warm_p50"].items() if v and self.v(k))
            mid = ratios[(len(ratios) + 1) // 2 - 1]
            out.append(f"\nThe lookups section replaced an earlier attempt, started {s['started_at']} at "
                       f"`{s['commit']}`, whose outside load peaked at {s['external_cpu_cores_max']} cores "
                       f"({(s.get('external_top_at_max') or ['-'])[0]}). Per lookup, the published warm plan p50 is "
                       f"{ratios[0]:.2f} to {ratios[-1]:.2f} times that attempt's (median {mid:.2f}, "
                       f"{len(ratios)} lookups; `context[\"lookups-previous-attempt\"]` in `perf.json`).")
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
                        # DuckDB's .timer has 1 ms resolution: whole ms.
                        f = lambda what, v: (f"{v:,.0f}" if what == "duckdb" and v is not None else fmt_ms(v))
                        cell = lambda what: (f"{f(what, self.v(f'{base}/{what}/{cache}/p50'))} / "
                                             f"{f(what, self.v(f'{base}/{what}/{cache}/p95'))}")
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
            out.append("| budget | wall (s, n = 1) | rows / s per side | passes | meter peak (MiB) | peak RSS (MiB) | peak footprint (MiB) | stated bound (MiB) |")
            out.append("|---|---:|---:|---:|---:|---:|---:|---:|")
            skipped = []
            for b in ("64M", "256M", "1G", "8G"):
                w = self.v(f"reconcile/{ds}/{b}/wall")
                if w is None:
                    nr = self.c.get(f"reconcile/{ds}/{b}", {}).get("not_run")
                    if nr:
                        skipped.append(f"{b}: not run, {nr}")
                    continue
                mem = self.c[f"reconcile/{ds}/{b}"]["memory"]
                bound = mem["budget_bytes"] + mem["peak_batch_bytes"] + (256 << 20)
                foot = self.v(f"reconcile/{ds}/{b}/peak-footprint")
                rss = self.v(f"reconcile/{ds}/{b}/peak-rss")
                over = " **(over)**" if max(rss, foot or 0) > bound else ""
                out.append(f"| {b} | {w / 1000:,.1f} | {self.v(f'reconcile/{ds}/{b}/rows-per-s-per-side'):,} | "
                           f"{self.v(f'reconcile/{ds}/{b}/passes')} | {mib(self.v(f'reconcile/{ds}/{b}/meter-peak'))} | "
                           f"{mib(rss)}{over} | {mib(foot) if foot is not None else 'not recorded'} | {mib(bound)} |")
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

    def expected(self):
        """The brief's expected hot paths against every profile."""
        profs = [(k.split("/", 1)[1], v) for k, v in self.c.items()
                 if k.startswith("profile/") and isinstance(v, dict) and "expected_paths" in v]
        if not profs:
            return ""
        out = ["| expected path | " + " | ".join(n for n, _ in profs) + " |",
               "|---|" + "---:|" * len(profs)]
        for path in profs[0][1]["expected_paths"]:
            out.append(f"| {path} | " + " | ".join(f"{v['expected_paths'].get(path, 0)}" for _, v in profs) + " |")
        return "\n".join(out)

    def profiles(self):
        out = []
        for k, v in self.c.items():
            if not k.startswith("profile/") or not isinstance(v, dict) or "top" not in v:
                continue
            name = k.split("/", 1)[1]
            idle = v["idle_weight"] / max(1, v["idle_weight"] + v["on_cpu_weight"]) * 100
            cpu = f"{v['cpu_seconds']:,.1f} s of CPU sampled" if v.get("cpu_seconds") is not None else "CPU sampled"
            out.append(f"#### {name}\n")
            out.append(f"`{v['command']}`, {v['iterations']} iteration(s) at {v['rate_hz']} Hz"
                       + (f", {v['passes']} passes" if "passes" in v else "")
                       + f"; {cpu}. Each sample's CPU delta is credited to the stack the thread had at the "
                       f"previous sample (the CPU was used in between). {idle:.0f}% of it is CPU used between two "
                       "samples that both found the thread waiting: the sampler did not see what ran. It is "
                       "listed apart; the self and inclusive tables rank the rest.\n")
            out.append("| # | self % (CPU not charged to waits) | frame |")
            out.append("|---:|---:|---|")
            for i, t in enumerate(v["top"], 1):
                frame = t["frame"].replace("|", "\\|")
                out.append(f"| {i} | {t['self_pct']} | `{frame}` |")
            u = v.get("unprofiled")
            if u:
                out.append(f"\nRun once more without the profiler under `/usr/bin/time -p`: {u['wall_s']:.2f} s wall, "
                           f"{u['user_s']:.2f} s user, {u['sys_s']:.2f} s system, so {u['cores_busy']:.2f} cores "
                           "busy on average.\n")
            if v.get("waits") and idle >= 5:
                out.append("\nCPU used between two samples that both found the thread waiting (% of all sampled CPU; "
                           "the sampler did not see what ran), by the wait frame and the code that waited:\n")
                for wt in v["waits"]:
                    out.append(f"- `{wt['frame']}` {wt['cpu_pct']}: " + ("; ".join(
                        f"`{c['frame']}` {c['cpu_pct']}" for c in wt["callers"]) or "no caller named"))
                out.append("")
            callers = v.get("callers") or {}
            if any(callers.values()):
                out.append("\nWho called the top self frames that are not bellwether's own: the nearest "
                           "bellwether function, or on a thread with none (a blocking-I/O pool) the nearest "
                           "named library function (% of the CPU not charged to waits):\n")
                for frame, cs in callers.items():
                    if cs:
                        out.append(f"- `{frame}`: " + "; ".join(f"`{c['frame']}` {c['pct']}" for c in cs))
            if v.get("inclusive"):
                out.append("\nbellwether functions by inclusive CPU, not charged to waits (every thread):\n")
                out.append("| # | total % (CPU not charged to waits) | without store reads % | function |")
                out.append("|---:|---:|---:|---|")
                for i, t in enumerate(v["inclusive"], 1):
                    nr = t.get("without_store_reads_pct", "-")
                    out.append(f"| {i} | {t['total_pct']} | {nr} | `{t['frame']}` |")
            if v.get("busiest_threads_cpu_pct"):
                out.append(f"\nCPU by thread: the busiest three of {v['threads']} threads hold "
                           + ", ".join(f"{x}%" for x in v["busiest_threads_cpu_pct"]) + " of the sampled CPU.")
            cover = None
            if u and u.get("wall_s"):
                cover = v["main_thread_wall_samples"] / (v["rate_hz"] * u["wall_s"] * v["iterations"])
            if v.get("main_thread_wall") and cover is not None and cover < 0.5:
                out.append(f"\nThe main thread has {v['main_thread_wall_samples']:,} samples, {cover:.0%} of its wall "
                           "time at this rate: samply records a sleeping thread only when it runs, so no "
                           "main-thread wall table is given.")
            elif v.get("main_thread_wall"):
                out.append(f"\nMain thread wall time ({v['main_thread_wall_samples']:,} samples, on CPU or "
                           "waiting), by bellwether function on the stack:\n")
                out.append("| # | wall % (main thread) | function |")
                out.append("|---:|---:|---|")
                for i, t in enumerate(v["main_thread_wall"], 1):
                    out.append(f"| {i} | {t['wall_pct']} | `{t['frame']}` |")
            out.append("")
        return "\n".join(out)


def lookup_rows(d):
    """(dataset, lookup, keys) of every lookup a perf.json measured, in its order."""
    rows = []
    for k in d.c:
        p = k.split("/")
        if p[0] == "lookup" and len(p) == 4 and p[3].startswith("k"):
            rows.append((p[1], p[2], int(p[3][1:])))
    return rows


def ratio(before, after):
    return "-" if not before or after is None else f"{after / before:.3f}"


def compare_lookups(before, after):
    """The matched plan p50 of two perf.json files, per lookup, against the
    full scan the after run measured on the same corpus; then, per corpus,
    whether every lookup's plan beat that scan (the WP22b headline)."""
    out = []
    verdicts = []
    rows = [r for r in lookup_rows(after) if r in set(lookup_rows(before))]
    for ds in dict.fromkeys(r[0] for r in rows):
        out.append(f"#### {ds}\n")
        for cache in ("warm", "cold"):
            scan = after.v(f"lookup/{ds}/full-scan/{cache}/p50")
            scan_before = before.v(f"lookup/{ds}/full-scan/{cache}/p50")
            out.append(f"{cache.capitalize()} plan p50 (ms). Full scan p50: {fmt_ms(scan_before)} in the "
                       f"before run, {fmt_ms(scan)} in the after run; the after plan is compared with the "
                       "after run's scan.\n")
            out.append("| lookup | keys | before | after | after / before | after plan < full scan |")
            out.append("|---|---:|---:|---:|---:|---|")
            met = []
            for _, name, k in (r for r in rows if r[0] == ds):
                b = before.v(f"lookup/{ds}/{name}/k{k}/plan/{cache}/p50")
                a = after.v(f"lookup/{ds}/{name}/k{k}/plan/{cache}/p50")
                ok = a is not None and scan is not None and a < scan
                met.append((ok, a))
                out.append(f"| `{name}` | {k:,} | {fmt_ms(b)} | {fmt_ms(a)} | {ratio(b, a)} | "
                           f"{'yes' if ok else 'no'} |")
            out.append("")
            slowest = max((a for _, a in met if a is not None), default=None)
            verdicts.append((ds, cache, sum(ok for ok, _ in met), len(met), slowest, scan))
    out.append("#### Headline: plan time under full-scan time\n")
    out.append("| corpus | cache | lookups whose plan p50 beat the scan | slowest plan p50 (ms) | "
               "full scan p50 (ms) | target |")
    out.append("|---|---|---:|---:|---:|---|")
    for ds, cache, n_ok, n, slowest, scan in verdicts:
        out.append(f"| {ds} | {cache} | {n_ok} of {n} | {fmt_ms(slowest)} | {fmt_ms(scan)} | "
                   f"{'met' if n_ok == n else 'not met'} |")
    return "\n".join(out)


def compare_metrics(before, after, prefixes):
    """Every metric under `prefixes` both files measured: before, after, and
    their ratio."""
    out = ["| metric | unit | before | after | after / before |", "|---|---|---:|---:|---:|"]
    for mid, m in after.m.items():
        if not mid.startswith(prefixes) or mid not in before.m:
            continue
        b, a = before.v(mid), after.v(mid)
        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
            continue
        out.append(f"| `{mid}` | {m.get('unit', '')} | {b:,.4g} | {a:,.4g} | {ratio(b, a)} |")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("perf")
    ap.add_argument("--section", default="all")
    ap.add_argument("--before", help="a perf.json of the same sections from the build before (with "
                    "--section compare-lookups or compare-other)")
    a = ap.parse_args()
    d = Doc(a.perf)
    if a.section.startswith("compare"):
        if not a.before:
            ap.error(f"--section {a.section} needs --before")
        before = Doc(a.before)
        print(f"<!-- generated by scripts/perf-report.py --section {a.section} -->\n")
        if a.section == "compare-lookups":
            print(compare_lookups(before, d))
        else:
            print(compare_metrics(before, d, ("build/", "reconcile/", "replay/", "ledger/", "latency/")))
        return
    for s in ("runs", "lookups", "build", "reconcile", "latency", "expected", "profiles"):
        if a.section in ("all", s):
            print(f"<!-- generated by scripts/perf-report.py --section {s} -->\n")
            print(getattr(d, s)())


if __name__ == "__main__":
    main()
