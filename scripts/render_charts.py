#!/usr/bin/env python3
"""Render the measured results into charts and tables. Never edit the outputs.

Reads every docs/measured/results/*.json (schema: docs/measured/results/README.md)
and writes, deterministically:

  docs/assets/results-<chart>-{light,dark}.svg   charts, GitHub light and dark
  <!-- results:<block>:begin --> ... end -->      tables inside README.md and
                                                  docs/**/*.md, replaced in place

The `showcase` source (scripts/showcase.sh) gets the bytes, replay, latency,
and reconcile charts and the README headline. Any other source gets one chart
per numeric metric (results-<source>-<metric>-*.svg) and a table for the block
`results:<source>`, wherever that marker appears.

  scripts/render_charts.py           write everything
  scripts/render_charts.py --check   exit 1 if any output differs from what the
                                     committed results would render (CI)

Standard library only.
"""

import argparse
import glob
import json
import math
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(REPO, "docs", "measured", "results")
ASSETS = os.path.join(REPO, "docs", "assets")
MEASURED_PAGE = "docs/measured/2026-10-25-showcase.md"

THEMES = {
    "light": {
        "bg": "#ffffff",
        "fg": "#1f2328",
        "muted": "#656d76",
        "grid": "#656d76",
        "full": "#8c959f",
        "duckdb": "#0969da",
        "bw": "#1a7f37",
        "bw2": "#4ac26b",
        "pred": "#8250df",
    },
    "dark": {
        "bg": "#0d1117",
        "fg": "#e6edf3",
        "muted": "#8b949e",
        "grid": "#8b949e",
        "full": "#6e7681",
        "duckdb": "#58a6ff",
        "bw": "#3fb950",
        "bw2": "#2ea043",
        "pred": "#a371f7",
    },
}
FONT = "-apple-system,Segoe UI,Helvetica,Arial,sans-serif"
DATASET_NAMES = {"taxi": "NYC taxi", "gh": "GH Archive", "clickbench": "ClickBench", "market": "Market (synthetic)"}


# ---- formatting -----------------------------------------------------------------
def human_bytes(n):
    n = float(n)
    for unit, scale in (("GB", 1e9), ("MB", 1e6), ("kB", 1e3)):
        if n >= scale:
            return f"{n / scale:.1f} {unit}"
    return f"{int(n)} B"


def human_ms(ms):
    ms = float(ms)
    if ms >= 1000:
        return f"{ms / 1000:.2f} s"
    if ms >= 10:
        return f"{ms:.0f} ms"
    return f"{ms:.1f} ms"


def human_rate(r):
    r = float(r)
    if r >= 1e6:
        return f"{r / 1e6:.1f} M rows/s"
    return f"{r / 1e3:.0f} k rows/s"


def commas(n):
    return f"{int(n):,}"


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def wrap(text, width):
    lines, cur = [], ""
    for w in text.split():
        if cur and len(cur) + 1 + len(w) > width:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}" if cur else w
    if cur:
        lines.append(cur)
    return lines


def f1(x):
    return f"{x:.1f}"


def ratio(a, b):
    """How many times fewer: a over b, as text."""
    if not b:
        return "–"
    r = a / b
    if r < 1.05:
        return "same" if r > 0.95 else f"{r:.2f}×"
    return f"{r:.0f}×" if r >= 10 else f"{r:.1f}×"


# ---- results ------------------------------------------------------------------
class Results:
    def __init__(self, doc):
        self.doc = doc
        self.rows = doc.get("rows", [])
        self.queries = doc.get("queries", {})
        self.index = {}
        for r in self.rows:
            self.index[(r["query"], r["side"], r["metric"])] = r

    def v(self, q, side, metric):
        r = self.index.get((q, side, metric))
        return None if r is None else r["value"]

    def ids(self, kinds=None):
        out = []
        for q, meta in self.queries.items():
            if kinds is None or meta.get("kind") in kinds:
                out.append(q)
        return out

    def provenance(self, qids=None):
        """The distinct machine, builds, and dates behind the given queries."""
        rows = [r for r in self.rows if qids is None or r["query"] in qids]
        machines = sorted({r["machine"] for r in rows})
        builds = sorted({r["build"] for r in rows if r["build"] != "footer statistics (no engine)"})
        dates = sorted({r["date"] for r in rows})
        return machines, builds, dates

    def provenance_line(self, qids=None):
        m, b, d = self.provenance(qids)
        return f"{'; '.join(m)}. {'; '.join(b)}. Measured {', '.join(d)}."

    def dataset_name(self, ds):
        meta = self.doc.get("datasets", {}).get(ds)
        if isinstance(meta, dict) and meta.get("name"):
            return meta["name"]
        return DATASET_NAMES.get(ds, ds)


# ---- SVG ----------------------------------------------------------------------
class Svg:
    def __init__(self, width, height, t):
        self.w, self.h, self.t = width, height, t
        self.parts = []

    def text(self, x, y, s, size=13, color="fg", anchor=None, weight=None):
        a = f' text-anchor="{anchor}"' if anchor else ""
        wt = f' font-weight="{weight}"' if weight else ""
        self.parts.append(
            f'<text x="{f1(x)}" y="{f1(y)}" fill="{self.t[color]}" font-size="{size}"{a}{wt}>{esc(s.replace("`", ""))}</text>'
        )

    def rich(self, x, y, strong, rest, size=13):
        self.parts.append(
            f'<text x="{f1(x)}" y="{f1(y)}" fill="{self.t["fg"]}" font-size="{size}">'
            f'<tspan font-weight="600">{esc(strong)}</tspan>'
            f'<tspan fill="{self.t["muted"]}" font-size="{size - 1}">{esc(rest)}</tspan></text>'
        )

    def rect(self, x, y, w, h, color, rx=3):
        self.parts.append(
            f'<rect x="{f1(x)}" y="{f1(y)}" width="{f1(max(w, 0.0))}" height="{f1(h)}" rx="{rx}" fill="{self.t[color]}"/>'
        )

    def vline(self, x, y1, y2):
        self.parts.append(
            f'<line x1="{f1(x)}" y1="{f1(y1)}" x2="{f1(x)}" y2="{f1(y2)}" stroke="{self.t["grid"]}"'
            ' stroke-opacity="0.25" stroke-dasharray="3 4"/>'
        )

    def render(self):
        head = (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" width="{self.w}"'
            f' height="{self.h}" font-family="{FONT}">\n'
            f'<rect width="{self.w}" height="{self.h}" fill="{self.t["bg"]}" rx="12"/>\n'
        )
        return head + "\n".join(self.parts) + "\n</svg>\n"


class Scale:
    def __init__(self, values, x0, x1, log):
        vals = [v for v in values if v and v > 0]
        self.log = log
        self.x0, self.x1 = x0, x1
        if log:
            self.lo = math.floor(math.log10(min(vals))) if vals else 0
            self.hi = math.ceil(math.log10(max(vals))) if vals else 1
            if self.hi == self.lo:
                self.hi += 1
        else:
            self.lo = 0
            top = max(vals) if vals else 1
            mag = 10 ** math.floor(math.log10(top))
            self.hi = math.ceil(top / mag) * mag

    def x(self, v):
        if v is None or v <= 0:
            return self.x0
        if self.log:
            f = (math.log10(v) - self.lo) / (self.hi - self.lo)
        else:
            f = v / self.hi
        return self.x0 + max(f, 0.0) * (self.x1 - self.x0)

    def ticks(self):
        if self.log:
            return [10**e for e in range(self.lo, self.hi + 1)]
        step = self.hi / 4
        return [step * i for i in range(5)]


def hbar_chart(theme, title, takeaway, subtitle, groups, series, fmt, log=True, tick_fmt=None, width=1100):
    """Horizontal grouped bars.

    groups: [{"header": str|None, "label": str, "sub": str, "bars": [(value, note)] per series}]
    series: [(name, color)]
    """
    t = THEMES[theme]
    label_right, x0, x1 = 430, 442, width - 220
    bar_h, gap, group_gap, header_h = 11, 3, 16, 26
    take = wrap(takeaway, 138)
    subs = wrap(subtitle, 176)
    y = 34
    legend_y = 34 + 24 + 22 * len(take) + 16 * len(subs) + 8
    head_h = legend_y + 26
    gh = len(series) * bar_h + (len(series) - 1) * gap
    group_h = max(gh, 30) + group_gap
    n_headers = sum(1 for g in groups if g.get("header"))
    plot_h = len(groups) * group_h + n_headers * header_h
    height = int(head_h + plot_h + 50)
    s = Svg(width, height, t)
    s.text(24, y, title, size=18, weight="600")
    for i, line in enumerate(take):
        s.text(24, y + 24 + 22 * i, line, size=15)
    y = y + 24 + 22 * len(take) - 4
    for i, line in enumerate(subs):
        s.text(24, y + 16 * i, line, size=12, color="muted")
    lx, ly = 24, legend_y
    for name, color in series:
        s.rect(lx, ly - 10, 12, 12, color, rx=2)
        s.text(lx + 18, ly, name, size=13)
        lx += 18 + 7.2 * len(name) + 28
    values = [b[0] for g in groups for b in g["bars"] if b[0] is not None]
    sc = Scale(values, x0, x1, log)
    top = head_h
    y = top
    for g in groups:
        if g.get("header"):
            s.text(24, y + 18, g["header"], size=14, weight="600")
            y += header_h
        s.text(label_right, y + 12, g["label"], size=13, anchor="end")
        if g.get("sub"):
            s.text(label_right, y + 27, g["sub"], size=11, color="muted", anchor="end")
        by = y
        for (value, note), (_, color) in zip(g["bars"], series):
            if value is not None:
                xe = sc.x(value)
                s.rect(x0, by, xe - x0, bar_h, color, rx=2)
                s.rich(xe + 6, by + bar_h - 1, fmt(value), f"  {note}" if note else "", size=11)
            else:
                s.text(x0 + 6, by + bar_h - 1, "not measured", size=11, color="muted")
            by += bar_h + gap
        y += group_h
    bottom = y
    for tv in sc.ticks():
        tx = sc.x(tv) if tv > 0 else x0
        s.vline(tx, top - 6, bottom)
        s.text(tx, bottom + 18, (tick_fmt or fmt)(tv) if tv > 0 else "0", size=11, color="muted", anchor="middle")
    s.text(x0, bottom + 36, "log scale" if log else "linear scale", size=11, color="muted")
    return s.render()


# ---- the showcase charts ----------------------------------------------------------------
def showcase_groups(res, qids, bars_fn, by_dataset=True):
    """One group per question: under dataset headings in the given order, or
    (by_dataset=False) with the dataset named on the question's second line."""
    groups, last_ds = [], None
    for q in qids:
        meta = res.queries[q]
        ds = meta["dataset"]
        sub = bars_fn.sub(q) if hasattr(bars_fn, "sub") else ""
        if not by_dataset:
            sub = f"{res.dataset_name(ds)} · {sub}" if sub else res.dataset_name(ds)
        groups.append(
            {
                "header": res.dataset_name(ds) if by_dataset and ds != last_ds else None,
                "label": meta["text"],
                "sub": sub,
                "bars": bars_fn(q),
            }
        )
        last_ds = ds
    return groups


def lookups(res):
    return [q for q in res.ids() if res.queries[q].get("kind") != "reconcile"]


def reduction(res, q):
    """How many times fewer bytes DuckDB read on Bellwether's plan than alone."""
    return res.v(q, "duckdb", "bytes") / max(res.v(q, "duckdb-on-plan", "bytes"), 1)


def wins(res):
    """Questions where the plan read at least 5% fewer bytes than DuckDB alone,
    biggest reduction first."""
    return sorted((q for q in lookups(res) if reduction(res, q) >= 1.05), key=lambda q: -reduction(res, q))


def no_gain(res):
    """The rest, in the order they were asked."""
    w = set(wins(res))
    return [q for q in lookups(res) if q not in w]


def by_impact(res):
    return wins(res) + no_gain(res)


def bytes_chart(res, theme):
    qids = wins(res)

    def bars(q):
        rg = lambda side: res.v(q, side, "row_groups")
        return [
            (res.v(q, "full-scan", "bytes"), f"{rg('full-scan')} row groups"),
            (res.v(q, "duckdb", "bytes"), f"{rg('duckdb')} row groups"),
            (res.v(q, "duckdb-on-plan", "bytes"), f"{rg('duckdb-on-plan')} row groups · {ratio(res.v(q, 'duckdb', 'bytes'), res.v(q, 'duckdb-on-plan', 'bytes'))} fewer"),
        ]

    bars.sub = lambda q: f"{returned(res, q)} returned"
    best = qids[0]
    m = res.queries[best]
    take = (
        f"For {m['text']} ({res.dataset_name(m['dataset'])}, {returned(res, best)}), DuckDB read "
        f"{human_bytes(res.v(best, 'duckdb-on-plan', 'bytes'))} following Bellwether's plan instead of "
        f"{human_bytes(res.v(best, 'duckdb', 'bytes'))} on its own; the plan read less on {len(qids)} of "
        f"{len(lookups(res))} questions, shown here biggest reduction first."
    )
    sub = (
        "Bytes and row groups read per question. Full scan: footer sums. DuckDB alone and on the plan: DuckDB's profiler, "
        "the same SQL, the plan's row groups selected by row number. " + res.provenance_line(qids)
    )
    return hbar_chart(
        theme,
        "Bytes read to answer each question",
        take,
        sub,
        showcase_groups(res, qids, bars, by_dataset=False),
        [("full scan", "full"), ("DuckDB alone", "duckdb"), ("DuckDB on Bellwether's plan", "bw")],
        human_bytes,
    )


def returned(res, q):
    n = res.v(q, "duckdb", "rows_returned")
    noun = res.queries[q].get("noun", "rows")
    if n == 1 and noun.endswith("s"):
        noun = noun[:-1]
    return f"{commas(n)} {noun}"


def same_count(res, qids):
    n = 0
    for q in qids:
        a, b = res.v(q, "duckdb", "bytes"), res.v(q, "duckdb-on-plan", "bytes")
        if a and b and a / b < 1.05:
            n += 1
    return n


def replay_ids(res):
    """Questions whose key `fix` applied (a prediction and a realized plan),
    biggest reduction first."""
    return [q for q in by_impact(res) if res.v(q, "bellwether", "fix") == "applied"]


def replay_chart(res, theme):
    qids = replay_ids(res)

    def bars(q):
        p, r = res.v(q, "bellwether-predicted", "bytes"), res.v(q, "bellwether-fixed", "bytes")
        outcome = res.v(q, "bellwether", "fix") or ""
        receipt = res.v(q, "bellwether", "receipt")
        note = f"receipt {receipt[:12]}" if receipt else outcome
        return [
            (p, f"{res.v(q, 'bellwether-predicted', 'row_groups')} row groups"),
            (r, ("= predicted · " if p == r else "≠ predicted · ") + note),
        ]

    bars.sub = lambda q: f"key {res.v(q, 'bellwether', 'key')}"
    equal = sum(1 for q in qids if res.v(q, "bellwether-predicted", "bytes") == res.v(q, "bellwether-fixed", "bytes"))
    take = (
        f"Before any index existed, replay predicted the bytes each fix would plan; after `fix` applied the key, "
        f"the plan read exactly the predicted bytes on {equal} of {len(qids)} questions"
        + (", to the byte." if equal == len(qids) else ".")
    )
    sub = "Predicted: `why` (exact replay over footer statistics). Planned: the lookup verb on the index `fix` built. " + res.provenance_line(qids)
    return hbar_chart(
        theme,
        "Predicted by replay vs planned after fix",
        take,
        sub,
        showcase_groups(res, qids, bars, by_dataset=False),
        [("predicted before the index", "pred"), ("planned after fix", "bw")],
        human_bytes,
    )


def latency_takeaway(res):
    qids = lookups(res)
    faster = [q for q in qids if res.v(q, "bellwether+duckdb", "wall_ms") < res.v(q, "duckdb", "wall_ms")]
    best = max(qids, key=lambda q: res.v(q, "duckdb", "wall_ms") / res.v(q, "bellwether+duckdb", "wall_ms"))
    return (
        f"Planning first and then reading only the planned row groups was faster than DuckDB alone on {len(faster)} of "
        f"{len(qids)} questions (best: {res.queries[best]['text']}, {human_ms(res.v(best, 'bellwether+duckdb', 'wall_ms'))} "
        f"instead of {human_ms(res.v(best, 'duckdb', 'wall_ms'))}); on small or already-pruned questions the planning step costs more than it saves."
    )


def latency_chart(res, theme):
    qids = by_impact(res)

    def bars(q):
        plan, onp = res.v(q, "bellwether-plan", "wall_ms"), res.v(q, "duckdb-on-plan", "wall_ms")
        return [
            (res.v(q, "duckdb", "wall_ms"), ""),
            (res.v(q, "bellwether+duckdb", "wall_ms"),
             f"plan {human_ms(plan)} + query {human_ms(onp)}" if res.v(q, "bellwether", "fix") != "refused: nothing_to_gain"
             else "no plan: fix refused, every row group read"),
        ]

    take = latency_takeaway(res)
    sub = (
        "Median of 3 warm runs. DuckDB: the profiler's query time. Bellwether: the lookup verb's wall time (whole process) "
        "plus DuckDB's query time on the plan. " + res.provenance_line(qids)
    )
    return hbar_chart(
        theme,
        "Time to answer each question",
        take,
        sub,
        showcase_groups(res, qids, bars, by_dataset=False),
        [("DuckDB alone", "duckdb"), ("Bellwether plan + DuckDB on the plan", "bw")],
        human_ms,
    )


def reconcile_takeaway(res):
    qids = res.ids({"reconcile"})
    planted = [q for q in qids if res.queries[q].get("planted")]
    found = [q for q in planted if res.v(q, "bellwether", "differing_keys") == 1]
    big = max(qids, key=lambda q: res.v(q, "bellwether", "rows_compared"))
    bwr, dkr = res.v(big, "bellwether", "rows_per_s"), res.v(big, "duckdb", "rows_per_s")
    cmp = (
        f"DuckDB's EXCEPT ALL is {dkr / bwr:.1f}× faster on the same files and writes no certificate"
        if dkr > bwr
        else f"{bwr / dkr:.1f}× DuckDB's EXCEPT ALL on the same files"
    )
    return (
        f"Bellwether compared {commas(res.v(big, 'bellwether', 'rows_compared'))} rows at {human_rate(bwr)} "
        f"and named the planted change in {len(found)} of {len(planted)} planted runs; {cmp}."
    )


def reconcile_chart(res, theme):
    qids = res.ids({"reconcile"})

    def bars(q):
        bw_found = res.v(q, "bellwether", "differing_keys")
        dk_found = res.v(q, "duckdb", "differing_rows")
        cert = res.v(q, "bellwether", "certificate") or ""
        return [
            (res.v(q, "bellwether", "rows_per_s"),
             f"{human_ms(res.v(q, 'bellwether', 'wall_ms'))} · {bw_found} differing key{'s' if bw_found != 1 else ''} · certificate {cert[:12]}"),
            (res.v(q, "duckdb", "rows_per_s"),
             f"{human_ms(res.v(q, 'duckdb', 'wall_ms'))} · {dk_found} differing row{'s' if dk_found != 1 else ''}"),
        ]

    bars.sub = lambda q: f"{commas(res.v(q, 'bellwether', 'rows_compared'))} rows, both sides"
    take = reconcile_takeaway(res)
    sub = (
        "Rows read from both sides per second, one cold-cache-free run each, default 256 MiB memory budget. "
        + res.provenance_line(qids)
    )
    return hbar_chart(
        theme,
        "Reconcile: rows checked per second",
        take,
        sub,
        showcase_groups(res, qids, bars),
        [("bellwether reconcile", "bw"), ("DuckDB EXCEPT ALL, both ways", "duckdb")],
        human_rate,
        log=False,
    )


CHART_IDS = {
    "results-bytes": wins,
    "results-replay": replay_ids,
    "results-latency": lookups,
    "results-reconcile": lambda res: res.ids({"reconcile"}),
}
SHOWCASE_CHARTS = {
    "results-bytes": bytes_chart,
    "results-replay": replay_chart,
    "results-latency": latency_chart,
    "results-reconcile": reconcile_chart,
}


# ---- generic charts and tables for other sources ------------------------------
def generic_outputs(source, res):
    """The charts a source declares in its top-level `charts` list.

    Each entry: {"name", "metric", "title", "takeaway"}, optionally "sides"
    (which sides, in bar order; default all) and "log" (default: log scale when
    the values span more than 30x). The takeaway is written by the source's
    script from its own numbers.
    """
    charts = {}
    for spec in res.doc.get("charts", []):
        metric = spec["metric"]
        rows = [
            r for r in res.rows
            if r["metric"] == metric and isinstance(r["value"], (int, float)) and not isinstance(r["value"], bool)
        ]
        if not rows:
            sys.exit(f"{source}.json: chart {spec['name']!r} names metric {metric!r}, which no numeric row has")
        units = {r["unit"] for r in rows}
        if len(units) != 1:
            sys.exit(f"{source}.json: metric {metric!r} has several units ({', '.join(sorted(units))}); use one")
        unit = units.pop()
        sides = list(spec.get("sides") or [])
        for r in rows:
            if not spec.get("sides") and r["side"] not in sides:
                sides.append(r["side"])
        keys = []
        for r in rows:
            k = (r["dataset"], r["query"])
            if k not in keys:
                keys.append(k)
        idx = {(r["dataset"], r["query"], r["side"]): r["value"] for r in rows}
        groups, last = [], None
        for ds, q in keys:
            text = res.queries.get(q, {}).get("text", q) if isinstance(res.queries.get(q), dict) else q
            groups.append(
                {
                    "header": res.dataset_name(ds) if ds != last else None,
                    "label": text,
                    "sub": "",
                    "bars": [(idx.get((ds, q, sd)), "") for sd in sides],
                }
            )
            last = ds
        vals = [v for v in idx.values() if v and v > 0]
        log = spec.get("log", bool(vals) and max(vals) / min(vals) > 30)
        fmt = human_bytes if unit == "bytes" else human_ms if unit == "ms" else human_rate if unit == "rows/s" else (
            lambda v, u=unit: f"{v:,.4g} {u}"
        )
        palette = ["duckdb", "bw", "full", "pred", "bw2"]
        series = [(sd, palette[i % len(palette)]) for i, sd in enumerate(sides)]
        prov = Results({"rows": rows}).provenance_line()
        name = f"results-{source}-{re.sub('[^a-z0-9]+', '-', spec['name'].lower()).strip('-')}"
        for theme in THEMES:
            charts[f"{name}-{theme}.svg"] = hbar_chart(
                theme, spec["title"], spec["takeaway"], f"{metric} ({unit}). {prov}", groups, series, fmt, log=log
            )
    return charts


def generic_table(res):
    lines = ["| Dataset | Query | Side | Metric | Value | Unit | Exactness |", "|---|---|---|---|---:|---|---|"]
    for r in res.rows:
        v = r["value"]
        v = f"{v:,}" if isinstance(v, int) and not isinstance(v, bool) else str(v)
        lines.append(f"| {r['dataset']} | {r['query']} | {r['side']} | {r['metric']} | {v} | {r['unit']} | {r['exactness']} |")
    lines.append("")
    lines.append(f"<sub>{Results({'rows': res.rows}).provenance_line()}</sub>")
    return "\n".join(lines)


# ---- markdown blocks ------------------------------------------------------------
def picture(name, alt, width=1000, prefix=""):
    return (
        '<p align="center">\n  <picture>\n'
        f'    <source media="(prefers-color-scheme: dark)" srcset="{prefix}docs/assets/{name}-dark.svg">\n'
        f'    <img alt="{esc(alt)}" src="{prefix}docs/assets/{name}-light.svg" width="{width}">\n'
        "  </picture>\n</p>"
    )


def headline(res, qids, how="How", bold=True):
    out = [
        f"| Dataset | Question | Rows | DuckDB alone | DuckDB on Bellwether's plan | Fewer bytes | Full scan | {how} |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for q in qids:
        m = res.queries[q]
        full, duck, plan = res.v(q, "full-scan", "bytes"), res.v(q, "duckdb", "bytes"), res.v(q, "duckdb-on-plan", "bytes")
        frg, drg, prg = res.v(q, "full-scan", "row_groups"), res.v(q, "duckdb", "row_groups"), res.v(q, "duckdb-on-plan", "row_groups")
        out.append(
            f"| {res.dataset_name(m['dataset'])} | {m['text']} | {commas(res.v(q, 'duckdb', 'rows_returned'))} | "
            f"{human_bytes(duck)} ({drg} rg) | {'**' if bold else ''}{human_bytes(plan)} ({prg} rg){'**' if bold else ''} | "
            f"{ratio(duck, plan)} | "
            f"{human_bytes(full)} ({frg} rg) | {why_text(res, q)} |"
        )
    return "\n".join(out)


def why_text(res, q):
    full_rg = res.v(q, "full-scan", "row_groups")
    duck_rg = res.v(q, "duckdb", "row_groups")
    plan_rg = res.v(q, "duckdb-on-plan", "row_groups")
    fixed_rg = res.v(q, "bellwether-fixed", "row_groups")
    outcome = res.v(q, "bellwether", "fix") or ""
    key = res.v(q, "bellwether", "key")
    if outcome.startswith("refused"):
        return f"no key can help: the keys sit in every row group (`fix` refused, {outcome.split(': ', 1)[-1]})"
    if fixed_rg == full_rg:
        return "no key can help: the keys sit in every row group"
    if fixed_rg >= 0.9 * full_rg:
        return f"no key can help much: the keys sit in {fixed_rg} of {full_rg} row groups"
    if plan_rg >= duck_rg:
        return f"DuckDB's min/max statistics already prune as well as `{key}`"
    if duck_rg == full_rg:
        return f"DuckDB's statistics prune nothing here; `{key}` plans {fixed_rg} of {full_rg} row groups"
    return f"DuckDB prunes to {duck_rg}; `{key}` plans {fixed_rg} of {full_rg} row groups"


def reconcile_table(res):
    out = [
        "| Run | Rows, both sides | `bellwether reconcile` | Found | Certificate | DuckDB `EXCEPT ALL`, both ways | Found |",
        "|---|---:|---:|---|---|---:|---|",
    ]
    for q in res.ids({"reconcile"}):
        k, r = res.v(q, "bellwether", "differing_keys"), res.v(q, "duckdb", "differing_rows")
        out.append(
            f"| {res.queries[q]['text']} | {commas(res.v(q, 'bellwether', 'rows_compared'))} | "
            f"{human_ms(res.v(q, 'bellwether', 'wall_ms'))} | {k} differing key{'s' if k != 1 else ''}"
            f"{', pass' if res.v(q, 'bellwether', 'pass') else ', fail'} | `{res.v(q, 'bellwether', 'certificate')[:12]}` | "
            f"{human_ms(res.v(q, 'duckdb', 'wall_ms'))} | {r} differing row{'s' if r != 1 else ''} |"
        )
    return "\n".join(out)


def no_gain_text(res, qids):
    refused = [q for q in qids if (res.v(q, "bellwether", "fix") or "").startswith("refused")]
    pruned = [q for q in qids if q not in refused and res.v(q, "duckdb", "row_groups") < res.v(q, "full-scan", "row_groups")]
    text = (
        "Bellwether measures this before anything is built: `why` prices every candidate key by exact replay over the "
        "footers and recommends none when none reads less, and `fix` refuses to attest a key whose replay saves nothing "
        f"(`nothing_to_gain`), as it did on {len(refused)} of these {len(qids)} questions."
    )
    if pruned:
        text += (
            " Where the data is already sorted by the key, DuckDB's own min/max statistics prune as well as any index, "
            "and the table says so."
        )
    return text + " The cause is the layout (the keys sit in nearly every row group), not a missing index."


def readme_block(res):
    qids = lookups(res)
    top, rest = wins(res), no_gain(res)
    datasets = []
    for q in qids:
        if res.queries[q]["dataset"] not in datasets:
            datasets.append(res.queries[q]["dataset"])
    synthetic = sum(1 for d in datasets if "synthetic" in res.dataset_name(d).lower())
    same = all(res.v(q, "duckdb-on-plan", "same_answer") is True for q in qids)
    parts = [
        "<!-- Generated by scripts/render_charts.py from docs/measured/results/showcase.json; do not edit by hand. -->",
        "",
        f"{len(qids)} questions people ask of {len(datasets)} datasets ({len(datasets) - synthetic} public, {synthetic} synthetic), "
        "each answered on the same files by DuckDB alone (with its own min/max pruning) and by DuckDB reading only the row "
        "groups Bellwether planned after `bellwether why` recommended a key and `bellwether fix` verified, applied, and "
        "attested it. Both answers are fingerprinted and must match"
        + ("; they did on every question." if same else "; see the measured page for the questions where they did not.")
        + f" Here are the {len(top)} where the plan read less, biggest reduction first (`rg` is row groups):",
        "",
        headline(res, top),
        "",
        f"<sub>{res.provenance_line(qids)} Full-scan and plan bytes are exact footer sums; DuckDB bytes are its profiler's "
        "`total_bytes_read`. Every number is a row of "
        "[`docs/measured/results/showcase.json`](docs/measured/results/showcase.json).</sub>",
        "",
        picture("results-bytes", "Bytes read per question: full scan, DuckDB alone, DuckDB on Bellwether's plan (log scale)", 1000),
        "",
        picture("results-replay", "Bytes predicted by replay before the index existed, against bytes planned after fix", 1000),
        "",
    ]
    recs = res.ids({"reconcile"})
    if recs:
        parts += [
            f"**Reconcile.** {reconcile_takeaway(res)}",
            "",
            reconcile_table(res),
            "",
        ]
    parts += [
        f"**Time.** {latency_takeaway(res)} Per-question times, the reconcile throughput chart, receipts, and the SQL: "
        f"[the measured page]({MEASURED_PAGE}).",
    ]
    if rest:
        parts += [
            "",
            "### When an index can't help, and how Bellwether tells you",
            "",
            no_gain_text(res, rest),
            "",
            headline(res, rest, how="Why not", bold=False),
        ]
    return "\n".join(parts)


def detail_block(res):
    qids = lookups(res)
    rows = ["### Bytes and row groups", "",
            "| Question | Full scan | DuckDB alone | Bellwether, unindexed | Bellwether, after fix | DuckDB on the plan | Same answer |",
            "|---|---:|---:|---:|---:|---:|---|"]
    for q in qids:
        cell = lambda side: f"{commas(res.v(q, side, 'bytes'))} ({res.v(q, side, 'row_groups')})"
        rows.append(
            f"| `{q}`: {res.queries[q]['text']} | {cell('full-scan')} | {cell('duckdb')} | {cell('bellwether-unindexed')} | "
            f"{cell('bellwether-fixed')} | {cell('duckdb-on-plan')} | "
            f"{'yes, ' + returned(res, q) if res.v(q, 'duckdb-on-plan', 'same_answer') else 'NO'} |"
        )
    rows += ["", "Bytes (row groups). Full scan and Bellwether columns are exact footer sums; DuckDB columns are its profiler's "
             "`total_bytes_read` and `total_row_groups_scanned`, which include footer reads.", "",
             "### `why` → `fix`: predicted against realized", "",
             "| Question | `why` recommended | Key put to `fix` | Predicted | Realized (fix's replay after apply) | Planned (lookup after fix) | `fix` | Receipt |",
             "|---|---|---|---:|---:|---:|---|---|"]
    for q in qids:
        pred = res.v(q, "bellwether-predicted", "bytes")
        real = res.v(q, "bellwether-realized", "bytes")
        rec = res.v(q, "bellwether", "receipt")
        rows.append(
            f"| `{q}` | `{res.v(q, 'bellwether', 'recommended')}` | `{res.v(q, 'bellwether', 'key')}` | "
            f"{commas(pred) if pred is not None else 'not priced'} | {commas(real) if real is not None else '–'} | "
            f"{commas(res.v(q, 'bellwether-fixed', 'bytes'))} | {res.v(q, 'bellwether', 'fix')} | "
            f"{'`' + rec + '`' if rec else '–'} |"
        )
    rows += ["", "### Time", "",
             "| Question | DuckDB alone | `why` | `fix` | Lookup plan | DuckDB on the plan | Plan + DuckDB on the plan |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for q in qids:
        refused = (res.v(q, "bellwether", "fix") or "").startswith("refused")
        ms = lambda side: (
            "– (no index)" if side == "bellwether-plan" and refused
            else human_ms(res.v(q, side, "wall_ms")) if res.v(q, side, "wall_ms") is not None else "–"
        )
        rows.append(
            f"| `{q}` | {ms('duckdb')} | {ms('bellwether-why')} | {ms('bellwether-fix')} | {ms('bellwether-plan')} | "
            f"{ms('duckdb-on-plan')} | {ms('bellwether+duckdb')} |"
        )
    rows += ["", "Median of 3 warm runs for DuckDB and the lookup; `why` and `fix` once each (they write records).", "",
             "### Reconcile", "",
             "| Run | Rows (both sides) | Bellwether | Differing keys | Passes | Pass | Certificate | DuckDB EXCEPT ALL | Differing rows |",
             "|---|---:|---:|---:|---:|---|---|---:|---:|"]
    for q in res.ids({"reconcile"}):
        rows.append(
            f"| `{q}`: {res.queries[q]['text']} | {commas(res.v(q, 'bellwether', 'rows_compared'))} | "
            f"{human_ms(res.v(q, 'bellwether', 'wall_ms'))}, {human_rate(res.v(q, 'bellwether', 'rows_per_s'))} | "
            f"{res.v(q, 'bellwether', 'differing_keys')} | {res.v(q, 'bellwether', 'passes')} | "
            f"{'yes' if res.v(q, 'bellwether', 'pass') else 'no'} | `{res.v(q, 'bellwether', 'certificate')}` | "
            f"{human_ms(res.v(q, 'duckdb', 'wall_ms'))}, {human_rate(res.v(q, 'duckdb', 'rows_per_s'))} | "
            f"{res.v(q, 'duckdb', 'differing_rows')} |"
        )
    rows += ["", "### The questions as SQL", ""]
    for q in res.ids():
        m = res.queries[q]
        rows.append(f"- `{q}` ({m.get('keys', '')}): `{m['sql']}`")
    rows += ["", f"<sub>{res.provenance_line()}</sub>"]
    return "\n".join(rows)


def replace_blocks(text, blocks):
    def sub(m):
        name = m.group(1)
        if name not in blocks:
            return m.group(0)
        return f"<!-- results:{name}:begin -->\n{blocks[name]}\n<!-- results:{name}:end -->"

    return re.sub(r"<!-- results:([a-z0-9-]+):begin -->.*?<!-- results:\1:end -->", sub, text, flags=re.S)


# ---- main -----------------------------------------------------------------------
def outputs():
    files, blocks = {}, {}
    for path in sorted(glob.glob(os.path.join(RESULTS, "*.json"))):
        doc = json.load(open(path))
        source = doc.get("source") or os.path.splitext(os.path.basename(path))[0]
        res = Results(doc)
        if source == "showcase":
            for name, fn in SHOWCASE_CHARTS.items():
                if not CHART_IDS[name](res):
                    continue
                for theme in THEMES:
                    files[os.path.join(ASSETS, f"{name}-{theme}.svg")] = fn(res, theme)
            blocks["showcase-headline"] = readme_block(res)
            blocks["showcase-detail"] = detail_block(res)
        else:
            for name, svg in generic_outputs(source, res).items():
                files[os.path.join(ASSETS, name)] = svg
            blocks[source] = generic_table(res)
    docs = [os.path.join(REPO, "README.md")] + sorted(glob.glob(os.path.join(REPO, "docs", "**", "*.md"), recursive=True))
    for path in docs:
        text = open(path).read()
        new = replace_blocks(text, blocks)
        if new != text:
            files[path] = new
    return files


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if any output is stale")
    a = ap.parse_args()
    files = outputs()
    stale = [p for p, c in files.items() if not os.path.exists(p) or open(p).read() != c]
    produced = set(files)
    orphans = [p for p in glob.glob(os.path.join(ASSETS, "results-*.svg")) if p not in produced]
    if a.check:
        for p in stale:
            print(f"stale: {os.path.relpath(p, REPO)} (run scripts/render_charts.py)")
        for p in orphans:
            print(f"orphan: {os.path.relpath(p, REPO)} (no results file renders it; delete it)")
        if stale or orphans:
            sys.exit(1)
        print(f"{len(files)} outputs match the committed results")
        return
    for p in stale:
        with open(p, "w") as f:
            f.write(files[p])
        print(f"wrote {os.path.relpath(p, REPO)}")
    for p in orphans:
        os.remove(p)
        print(f"removed {os.path.relpath(p, REPO)}")


if __name__ == "__main__":
    main()
