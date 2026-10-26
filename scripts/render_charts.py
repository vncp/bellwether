#!/usr/bin/env python3
"""Render the measured results into charts and tables. Never edit the outputs.

Reads every docs/measured/results/*.json (schema: docs/measured/results/README.md)
and writes, deterministically:

  docs/assets/results-<chart>-{light,dark}.svg   charts, GitHub light and dark
  <!-- results:<block>:begin --> ... end -->      tables inside README.md and
                                                  docs/**/*.md, replaced in place

The `showcase` source (scripts/showcase.sh) gets the bytes, replay, latency,
and reconcile charts, the README headline, and the measured page's tables. Any
other source gets the charts it declares in its `charts` list
(results-<source>-<name>-*.svg) and a table of every row for the block
`results:<source>`, wherever that marker appears.

  scripts/render_charts.py           write everything
  scripts/render_charts.py --check   exit 1 if any output differs from what the
                                     committed results would render, or a marker
                                     names a source with no results file (CI)

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
MEASURED_PAGE = "docs/measured/showcase.md"

THEMES = {
    "light": {
        "bg": "#ffffff",
        "fg": "#1f2328",
        "muted": "#656d76",
        "grid": "#656d76",
        "full": "#8c959f",
        "duckdb": "#0969da",
        "bw": "#1a7f37",
        "pred": "#8250df",
        "extra": "#bc4c00",
    },
    "dark": {
        "bg": "#0d1117",
        "fg": "#e6edf3",
        "muted": "#8b949e",
        "grid": "#8b949e",
        "full": "#6e7681",
        "duckdb": "#58a6ff",
        "bw": "#3fb950",
        "pred": "#a371f7",
        "extra": "#f0883e",
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
    def __init__(self, doc, name="results"):
        self.doc = doc
        self.rows = doc.get("rows", [])
        self.queries = doc.get("queries", {})
        self.index = {}
        for r in self.rows:
            key = (r["dataset"], r["query"], r["side"], r["metric"])
            if key in self.index:
                sys.exit(f"{name}: two rows for dataset {key[0]!r}, query {key[1]!r}, side {key[2]!r}, metric {key[3]!r}")
            self.index[key] = r

    def v(self, q, side, metric, ds=None):
        ds = ds or self.queries[q]["dataset"]
        r = self.index.get((ds, q, side, metric))
        return None if r is None else r["value"]

    def ids(self, kinds=None):
        return [q for q, meta in self.queries.items() if kinds is None or meta.get("kind") in kinds]

    def provenance_line(self, qids=None, load=False):
        """The machine, builds, and dates behind the given queries (and, for
        timings, the range of load averages recorded while measuring)."""
        rows = [r for r in self.rows if qids is None or r["query"] in qids]
        machines = sorted({r["machine"] for r in rows})
        builds = sorted({r["build"] for r in rows if r["build"] != "footer statistics (no engine)"})
        dates = sorted({r["date"] for r in rows})
        line = f"{'; '.join(machines)}. {'; '.join(builds)}. Measured {', '.join(dates)}."
        if load and self.load_range(qids):
            lo, hi, cores = self.load_range(qids)
            line += f" 1-minute load average while timing: {lo:.1f} to {hi:.1f} on {cores} cores."
        return line

    def load_range(self, qids=None):
        """(lowest, highest, cores) of the load averages recorded in `notes`."""
        loads = []
        for r in self.rows:
            m = re.match(r"1-minute load average ([0-9.]+) on (\d+) cores", r.get("notes") or "")
            if m and (qids is None or r["query"] in qids):
                loads.append((float(m.group(1)), m.group(2)))
        if not loads:
            return None
        return min(x for x, _ in loads), max(x for x, _ in loads), loads[0][1]

    def dataset(self, ds):
        meta = self.doc.get("datasets", {}).get(ds)
        return meta if isinstance(meta, dict) else {}

    def dataset_name(self, ds):
        return self.dataset(ds).get("name") or DATASET_NAMES.get(ds, ds)


def rg_text(n):
    return f"{commas(n)} row group{'' if n == 1 else 's'}"


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
            self.hi = next(m * mag for m in (1, 1.2, 1.6, 2, 2.4, 3, 4, 5, 6, 8, 10) if m * mag >= top)

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


def hbar_chart(theme, title, takeaway, subtitle, groups, series, fmt, log=True, tick_fmt=None):
    """Horizontal grouped bars.

    groups: [{"header": str|None, "label": str, "sub": str, "bars": [(value, note)] per series}]
    series: [(name, color)]. A bar whose value is None prints its note (or
    "not measured") instead. Groups with no value at all are dropped. The label
    column is as wide as the longest label.
    """
    t = THEMES[theme]
    groups = [g for g in groups if any(v is not None for v, _ in g["bars"])]
    longest = max([len(g["label"]) * 7.0 for g in groups] + [len(g.get("sub") or "") * 6.0 for g in groups] + [200])
    label_right = 24 + longest
    x0 = label_right + 12
    width = int(max(1100, x0 + 520 + 230))
    x1 = width - 230
    bar_h, gap, group_gap, header_h = 11, 3, 16, 26
    take = wrap(takeaway, int((width - 48) / 7.6))
    subs = wrap(subtitle, int((width - 48) / 6.0))
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
                s.text(x0 + 6, by + bar_h - 1, note or "not measured", size=11, color="muted")
            by += bar_h + gap
        y += group_h
    bottom = y
    for tv in sc.ticks():
        tx = sc.x(tv) if tv > 0 else x0
        s.vline(tx, top - 6, bottom)
        s.text(tx, bottom + 18, (tick_fmt or fmt)(tv) if tv > 0 else "0", size=11, color="muted", anchor="middle")
    s.text(x0, bottom + 36, "log scale" if log else "linear scale", size=11, color="muted")
    return s.render()


# ---- the showcase: which questions are which ------------------------------------
def lookups(res):
    return [q for q in res.ids() if res.queries[q].get("kind") != "reconcile"]


def outcome(res, q):
    return res.v(q, "bellwether", "fix") or ""


def planned(res, q):
    """`fix` (or, for the bitemporal key, `index`) left an index, so there is a
    plan and DuckDB ran on it."""
    return res.v(q, "duckdb-on-plan", "bytes") is not None


def reduction(res, q):
    """How many times fewer bytes DuckDB read on Bellwether's plan than alone."""
    return res.v(q, "duckdb", "bytes") / max(res.v(q, "duckdb-on-plan", "bytes"), 1)


def wins(res):
    """Questions with a plan that read at least 5% fewer bytes than DuckDB
    alone, biggest reduction first."""
    return sorted((q for q in lookups(res) if planned(res, q) and reduction(res, q) >= 1.05), key=lambda q: -reduction(res, q))


def no_gain(res):
    """The rest, in the order they were asked."""
    w = set(wins(res))
    return [q for q in lookups(res) if q not in w]


def by_impact(res):
    return wins(res) + no_gain(res)


def receipted(res):
    """Questions whose key `fix` verified, applied, and attested, biggest
    reduction first."""
    return [q for q in by_impact(res) if outcome(res, q) == "applied"]


def returned(res, q):
    n = res.v(q, "duckdb", "rows_returned")
    noun = res.queries[q].get("noun", "rows")
    if n == 1 and noun.endswith("s"):
        noun = noun[:-1]
    return f"{commas(n)} {noun}"


def faster(res, q):
    return res.v(q, "bellwether+duckdb", "wall_ms") < 0.95 * res.v(q, "duckdb", "wall_ms")


def groups_for(res, qids, bars):
    """One group per question, the dataset named on its second line."""
    out = []
    for q in qids:
        meta = res.queries[q]
        sub = bars.sub(q) if hasattr(bars, "sub") else ""
        name = res.dataset_name(meta["dataset"])
        out.append({"header": None, "label": meta["text"], "sub": f"{name} · {sub}" if sub else name, "bars": bars(q)})
    return out


# ---- the showcase charts and their takeaways -------------------------------------
def bytes_takeaway(res):
    qids = wins(res)
    best = qids[0]
    m = res.queries[best]
    return (
        f"For {m['text']} ({res.dataset_name(m['dataset'])}, {returned(res, best)}), DuckDB read "
        f"{human_bytes(res.v(best, 'duckdb-on-plan', 'bytes'))} following Bellwether's plan instead of "
        f"{human_bytes(res.v(best, 'duckdb', 'bytes'))} on its own. The plan read at least 5% less on {len(qids)} of "
        f"{len(lookups(res))} questions; these are those {len(qids)}, biggest reduction first."
    )


def bytes_chart(res, theme):
    qids = wins(res)

    def bars(q):
        rg = lambda side: rg_text(res.v(q, side, "row_groups"))
        return [
            (res.v(q, "full-scan", "bytes"), rg("full-scan")),
            (res.v(q, "duckdb", "bytes"), rg("duckdb")),
            (res.v(q, "duckdb-on-plan", "bytes"), f"{rg('duckdb-on-plan')} · {ratio(res.v(q, 'duckdb', 'bytes'), res.v(q, 'duckdb-on-plan', 'bytes'))} fewer"),
        ]

    bars.sub = lambda q: f"{returned(res, q)} returned"
    sub = (
        "Bytes and row groups read. Full scan: footer sums. DuckDB alone and on the plan: DuckDB's profiler, which also "
        "counts the footers it reads; the plan's row groups are selected by row number in the same SQL. "
        + res.provenance_line(qids)
    )
    return hbar_chart(
        theme,
        f"Bytes read on the {len(qids)} questions where the plan read less",
        bytes_takeaway(res),
        sub,
        groups_for(res, qids, bars),
        [("full scan", "full"), ("DuckDB alone", "duckdb"), ("DuckDB on Bellwether's plan", "bw")],
        human_bytes,
    )


def replay_takeaway(res):
    qids = receipted(res)
    equal = sum(1 for q in qids if res.v(q, "bellwether-predicted", "bytes") == res.v(q, "bellwether-fixed", "bytes"))
    return (
        "Before any index existed, `why` predicted by replay the bytes each key would plan; after `fix` applied it, "
        f"the plan read exactly the predicted bytes on {equal} of the {len(qids)} questions with a receipt"
        + (", to the byte." if equal == len(qids) else ".")
    )


def replay_chart(res, theme):
    qids = receipted(res)

    def bars(q):
        p, r = res.v(q, "bellwether-predicted", "bytes"), res.v(q, "bellwether-fixed", "bytes")
        return [
            (p, rg_text(res.v(q, "bellwether-predicted", "row_groups"))),
            (r, ("= predicted · " if p == r else "≠ predicted · ") + f"receipt {res.v(q, 'bellwether', 'receipt')[:12]}"),
        ]

    bars.sub = lambda q: f"key {res.v(q, 'bellwether', 'key')}"
    sub = (
        "Predicted: `why` (exact replay over footer statistics). Planned: the lookup verb on the index `fix` built "
        "(exact footer sums). " + res.provenance_line(qids)
    )
    return hbar_chart(
        theme,
        "Predicted by replay vs planned after fix",
        replay_takeaway(res),
        sub,
        groups_for(res, qids, bars),
        [("predicted before the index", "pred"), ("planned after fix", "bw")],
        human_bytes,
    )


def latency_takeaway(res):
    qids = [q for q in lookups(res) if planned(res, q)]
    fast = [q for q in qids if faster(res, q)]
    slow = [q for q in qids if res.v(q, "bellwether+duckdb", "wall_ms") > 1.05 * res.v(q, "duckdb", "wall_ms")]
    slow_read = [q for q in slow if res.v(q, "duckdb-on-plan", "wall_ms") > 1.05 * res.v(q, "duckdb", "wall_ms")]
    slow_plan = [q for q in slow if q not in slow_read]
    parts = [f"Bellwether's plan plus DuckDB on the plan was faster than DuckDB alone on {len(fast)} of the {len(qids)} questions with a plan"]
    if fast:
        best = max(fast, key=lambda q: res.v(q, "duckdb", "wall_ms") / res.v(q, "bellwether+duckdb", "wall_ms"))
        parts[0] += (
            f" ({res.queries[best]['text']}: {human_ms(res.v(best, 'bellwether+duckdb', 'wall_ms'))} against "
            f"{human_ms(res.v(best, 'duckdb', 'wall_ms'))})"
        )
    parts[0] += "."
    if slow_plan:
        plan_ms = [res.v(q, "bellwether-plan", "wall_ms") for q in slow_plan]
        parts.append(
            f"On {len(slow_plan)}, DuckDB on the plan took no more than 5% longer than DuckDB alone, and often less, but "
            f"the plan step (one `bellwether` process, {human_ms(min(plan_ms))} to {human_ms(max(plan_ms))}) cost "
            "more than that saved."
        )
    if slow_read:
        ex = max(slow_read, key=lambda q: res.v(q, "duckdb-on-plan", "wall_ms") / res.v(q, "duckdb", "wall_ms"))
        parts.append(
            f"On {len(slow_read)}, DuckDB read the plan's row-number ranges more slowly than it scans on its own "
            f"({res.queries[ex]['text']}: {rg_text(res.v(ex, 'duckdb-on-plan', 'row_groups'))} in "
            f"{human_ms(res.v(ex, 'duckdb-on-plan', 'wall_ms'))} on the plan against "
            f"{rg_text(res.v(ex, 'duckdb', 'row_groups'))} in {human_ms(res.v(ex, 'duckdb', 'wall_ms'))} alone)."
        )
    return " ".join(parts)


def latency_chart(res, theme):
    qids = by_impact(res)

    def bars(q):
        if not planned(res, q):
            return [(res.v(q, "duckdb", "wall_ms"), ""), (None, "no plan: fix refused, so there is nothing to time")]
        plan, onp = res.v(q, "bellwether-plan", "wall_ms"), res.v(q, "duckdb-on-plan", "wall_ms")
        return [
            (res.v(q, "duckdb", "wall_ms"), ""),
            (res.v(q, "bellwether+duckdb", "wall_ms"), f"plan {human_ms(plan)} + query {human_ms(onp)}"),
        ]

    sub = (
        "Median of 3 warm runs. DuckDB: the profiler's query time. Bellwether: the lookup verb's wall time (a whole "
        "process) plus DuckDB's query time on the plan. " + res.provenance_line(qids, load=True)
    )
    return hbar_chart(
        theme,
        "Time to answer each question",
        latency_takeaway(res),
        sub,
        groups_for(res, qids, bars),
        [("DuckDB alone", "duckdb"), ("Bellwether plan + DuckDB on the plan", "bw")],
        human_ms,
    )


def reconcile_takeaway(res):
    qids = res.ids({"reconcile"})
    planted = [q for q in qids if res.queries[q].get("planted")]
    found = [q for q in planted if res.v(q, "bellwether", "found_planted") is True]
    big = max(qids, key=lambda q: res.v(q, "bellwether", "rows_compared"))
    times = [res.v(q, "duckdb", "rows_per_s") / res.v(q, "bellwether", "rows_per_s") for q in qids]
    if min(times) > 1:
        cmp = f"DuckDB's EXCEPT ALL was {min(times):.1f}× to {max(times):.1f}× faster on the same files, and writes no certificate"
    elif max(times) < 1:
        cmp = f"it was {1 / max(times):.1f}× to {1 / min(times):.1f}× faster than DuckDB's EXCEPT ALL on the same files"
    else:
        cmp = f"DuckDB's EXCEPT ALL took {min(times):.2f}× to {max(times):.2f}× its time on the same files"
    load = res.load_range(qids)
    under = f" (timed at a 1-minute load average of {load[0]:.1f} to {load[1]:.1f} on {load[2]} cores)" if load else ""
    return (
        f"Bellwether compared up to {commas(res.v(big, 'bellwether', 'rows_compared'))} rows (both sides) at "
        f"{human_rate(res.v(big, 'bellwether', 'rows_per_s'))} and reported exactly the planted key in {len(found)} of "
        f"{len(planted)} planted runs; {cmp}{under}."
    )


def reconcile_chart(res, theme):
    qids = res.ids({"reconcile"})

    def bars(q):
        k, r = res.v(q, "bellwether", "differing_keys"), res.v(q, "duckdb", "differing_rows")
        return [
            (res.v(q, "bellwether", "rows_per_s"),
             f"{human_ms(res.v(q, 'bellwether', 'wall_ms'))} · {k} differing key{'s' if k != 1 else ''}"),
            (res.v(q, "duckdb", "rows_per_s"),
             f"{human_ms(res.v(q, 'duckdb', 'wall_ms'))} · {r} differing row{'s' if r != 1 else ''}"),
        ]

    bars.sub = lambda q: f"{commas(res.v(q, 'bellwether', 'rows_compared'))} rows, both sides"
    sub = (
        "Rows of both copies compared per second, one warm run each; Bellwether at its default 256 MiB memory budget, "
        "including writing its certificate. " + res.provenance_line(qids, load=True)
    )
    return hbar_chart(
        theme,
        "Reconcile: rows checked per second",
        reconcile_takeaway(res),
        sub,
        groups_for(res, qids, bars),
        [("bellwether reconcile", "bw"), ("DuckDB EXCEPT ALL, both ways", "duckdb")],
        human_rate,
        log=False,
    )


SHOWCASE_CHARTS = {
    # name: (which questions it draws, the chart, its takeaway for alt text)
    "results-bytes": (wins, bytes_chart, bytes_takeaway),
    "results-replay": (receipted, replay_chart, replay_takeaway),
    "results-latency": (by_impact, latency_chart, latency_takeaway),
    "results-reconcile": (lambda res: res.ids({"reconcile"}), reconcile_chart, reconcile_takeaway),
}


# ---- generic charts and tables for other sources ------------------------------
# A side keeps its colour in every chart; sides not named here take the
# remaining colours in order of first appearance.
SIDE_COLORS = {
    "full-scan": "full",
    "duckdb": "duckdb",
    "duckdb-on-plan": "bw",
    "bellwether-end-to-end": "bw",
    "bellwether-plan": "pred",
}
PALETTE = ["bw", "pred", "extra", "duckdb", "full"]


def side_colors(sides):
    used = {SIDE_COLORS[sd] for sd in sides if sd in SIDE_COLORS}
    free = [c for c in PALETTE if c not in used] or PALETTE
    out, i = [], 0
    for sd in sides:
        if sd in SIDE_COLORS:
            out.append((sd, SIDE_COLORS[sd]))
        else:
            out.append((sd, free[i % len(free)]))
            i += 1
    return out


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
            bars = [(idx.get((ds, q, sd)), "") for sd in sides]
            if all(v is None for v, _ in bars):
                continue
            meta = res.queries.get(q)
            groups.append(
                {
                    "header": res.dataset_name(ds) if ds != last else None,
                    "label": meta.get("text", q) if isinstance(meta, dict) else q,
                    "sub": "",
                    "bars": bars,
                }
            )
            last = ds
        vals = [b[0] for g in groups for b in g["bars"] if b[0] and b[0] > 0]
        log = spec.get("log", bool(vals) and max(vals) / min(vals) > 30)
        fmt = human_bytes if unit == "bytes" else human_ms if unit == "ms" else human_rate if unit == "rows/s" else (
            lambda v, u=unit: f"{v:,.0f} {u}" if v >= 1000 else f"{v:,.3g} {u}"
        )
        prov = Results({"rows": rows}, source).provenance_line(load=True)
        name = f"results-{source}-{re.sub('[^a-z0-9]+', '-', spec['name'].lower()).strip('-')}"
        for theme in THEMES:
            charts[f"{name}-{theme}.svg"] = hbar_chart(
                theme, spec["title"], spec["takeaway"], f"{metric} ({unit}). {prov}", groups, side_colors(sides), fmt, log=log
            )
    return charts


def generic_table(res):
    lines = ["| Dataset | Query | Side | Metric | Value | Unit | Exactness |", "|---|---|---|---|---:|---|---|"]
    for r in res.rows:
        v = r["value"]
        v = f"{v:,}" if isinstance(v, int) and not isinstance(v, bool) else str(v)
        lines.append(f"| {r['dataset']} | {r['query']} | {r['side']} | {r['metric']} | {v} | {r['unit']} | {r['exactness']} |")
    lines.append("")
    lines.append(f"<sub>{res.provenance_line(load=True)}</sub>")
    return "\n".join(lines)


# ---- markdown blocks ------------------------------------------------------------
def picture(name, alt, width=1000, base="docs/assets"):
    return (
        '<p align="center">\n  <picture>\n'
        f'    <source media="(prefers-color-scheme: dark)" srcset="{base}/{name}-dark.svg">\n'
        f'    <img alt="{esc(alt.replace("`", ""))}" src="{base}/{name}-light.svg" width="{width}">\n'
        "  </picture>\n</p>"
    )


def bytes_cell(res, q, side):
    return f"{human_bytes(res.v(q, side, 'bytes'))} ({res.v(q, side, 'row_groups')} rg)"


def headline(res, qids, how):
    out = [
        f"| Dataset | Question | Rows | DuckDB alone | DuckDB on Bellwether's plan | Fewer bytes | Full scan | {how} |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for q in qids:
        m = res.queries[q]
        if planned(res, q):
            plan = bytes_cell(res, q, "duckdb-on-plan")
            plan = f"**{plan}**" if q in wins(res) else plan
            fewer = ratio(res.v(q, "duckdb", "bytes"), res.v(q, "duckdb-on-plan", "bytes"))
        else:
            plan, fewer = "no plan (`fix` refused)", "–"
        out.append(
            f"| {res.dataset_name(m['dataset'])} | {m['text']} | {commas(res.v(q, 'duckdb', 'rows_returned'))} | "
            f"{bytes_cell(res, q, 'duckdb')} | {plan} | {fewer} | {bytes_cell(res, q, 'full-scan')} | {why_text(res, q)} |"
        )
    return "\n".join(out)


def pruning_cause(res, q):
    """Why DuckDB alone read what it read, from the footers' facts."""
    full_rg, duck_rg = res.v(q, "full-scan", "row_groups"), res.v(q, "duckdb", "row_groups")
    meta = res.dataset(res.queries[q]["dataset"])
    if duck_rg >= full_rg:
        if meta.get("column_chunks_with_minmax") == 0:
            return "the files carry no min/max statistics on any column, so DuckDB reads every row group"
        return "DuckDB's min/max statistics skip no row group for these keys"
    return f"DuckDB's min/max statistics skip to {duck_rg} row groups"


def why_text(res, q):
    full_rg = res.v(q, "full-scan", "row_groups")
    fixed_rg = res.v(q, "bellwether-fixed", "row_groups")
    pred_rg = res.v(q, "bellwether-predicted", "row_groups")
    out = outcome(res, q)
    key = res.v(q, "bellwether", "key")
    if out.startswith("refused"):
        return (
            f"the keys sit in {pred_rg} of {full_rg} row groups, so `why` recommended no key and `fix` refused "
            f"(`{out.split(': ', 1)[-1]}`)"
        )
    if out.startswith("not run"):
        return (
            f"{pruning_cause(res, q)}; `{key}` plans {fixed_rg} of {full_rg}. Bitemporal keys cannot be proposed or "
            "receipted until storage version 2, so this key was built with `index` and has no receipt"
        )
    if q in wins(res):
        return f"{pruning_cause(res, q)}; `{key}` plans {fixed_rg} of {full_rg}"
    unindexed = res.v(q, "bellwether-unindexed", "bytes")
    saved = 1 - res.v(q, "bellwether-predicted", "bytes") / unindexed
    if res.v(q, "duckdb", "row_groups") <= res.v(q, "duckdb-on-plan", "row_groups"):
        return (
            f"{pruning_cause(res, q)}, as many as `{key}` plans; `why` compared the key against a full scan "
            f"({res.v(q, 'bellwether-unindexed', 'row_groups')} row groups), not that min/max baseline, so it recommended "
            "the key and `fix` attested it"
        )
    return (
        f"the keys sit in {fixed_rg} of {full_rg} row groups; `why` still recommended `{key}`, which saves "
        f"{saved:.2%} of the bytes, and `fix` attested it"
    )


def reconcile_table(res):
    out = [
        "| Run | Rows, both sides | `bellwether reconcile` | Reported | DuckDB `EXCEPT ALL`, both ways | Reported |",
        "|---|---:|---:|---|---:|---|",
    ]
    for q in res.ids({"reconcile"}):
        k, r = res.v(q, "bellwether", "differing_keys"), res.v(q, "duckdb", "differing_rows")
        key = res.v(q, "bellwether", "differing_key")
        found = f"{k} differing key{'s' if k != 1 else ''}"
        if key:
            found += f": `{key}`" + (", the planted one" if res.v(q, "bellwether", "found_planted") else "")
        out.append(
            f"| {res.queries[q]['text']} | {commas(res.v(q, 'bellwether', 'rows_compared'))} | "
            f"{human_ms(res.v(q, 'bellwether', 'wall_ms'))} | {found} | "
            f"{human_ms(res.v(q, 'duckdb', 'wall_ms'))} | {r} differing row{'s' if r != 1 else ''} |"
        )
    out += ["", "A changed row is one differing key to `reconcile` and two rows to `EXCEPT ALL` (the old version from one side, "
            "the new from the other). `reconcile` writes a certificate holding the counts; the differing keys are in its "
            "`--json` output, and the script fails unless the reported key is the planted one. DuckDB's time is one query; "
            "Bellwether's includes writing the certificate."]
    return "\n".join(out)


def no_gain_text(res, qids):
    refused = [q for q in qids if outcome(res, q).startswith("refused")]
    attested = [q for q in qids if outcome(res, q) == "applied"]
    text = (
        f"On {len(refused)} of these {len(qids)} questions Bellwether said so before building anything: `why` priced "
        "every candidate key by exact replay over the footers, recommended none, and `fix` refused to attest a key that "
        "saves nothing (`nothing_to_gain`)."
    )
    if attested:
        text += (
            f" On the other {len(attested)} it did not: `why` recommended a key and `fix` attested it, though DuckDB on the "
            "plan read about as much as DuckDB alone. The last column gives each cause."
        )
    return text


def readme_block(res):
    qids = lookups(res)
    top, rest = wins(res), no_gain(res)
    datasets = []
    for q in qids:
        if res.queries[q]["dataset"] not in datasets:
            datasets.append(res.queries[q]["dataset"])
    synthetic = sum(1 for d in datasets if "synthetic" in res.dataset_name(d).lower())
    same = all(res.v(q, "duckdb-on-plan", "same_answer") is True for q in qids if planned(res, q))
    exceptions = [q for q in qids if outcome(res, q) != "applied"]
    parts = [
        "<!-- Generated by scripts/render_charts.py from docs/measured/results/showcase.json; do not edit by hand. -->",
        "",
        f"{len(qids)} questions people ask of {len(datasets)} datasets ({len(datasets) - synthetic} public, {synthetic} synthetic). "
        "Each is answered on the same files by DuckDB alone (with its own min/max pruning) and by DuckDB reading only "
        "the row groups Bellwether planned after `bellwether why` recommended a key and `bellwether fix` verified, "
        f"applied, and attested it; {len(exceptions)} questions are exceptions, each marked in its row. Both DuckDB answers "
        "are fingerprinted and must match"
        + ("; they did on every question with a plan." if same else "; see the measured page for those that did not.")
        + f" These are the {len(top)} where the plan read at least 5% less, biggest reduction first (`rg` is row groups):",
        "",
        headline(res, top, "How"),
        "",
        f"<sub>{res.provenance_line(qids, load=True)} Full scan: exact footer sums. Both DuckDB columns, the plan column included, are "
        "DuckDB's profiler (`total_bytes_read`), which counts the footers it reads too, so DuckDB alone can exceed the "
        f"full scan. The plan's exact footer sums are on [the measured page]({MEASURED_PAGE}). Every number is a row of "
        "[`docs/measured/results/showcase.json`](docs/measured/results/showcase.json).</sub>",
        "",
        picture("results-bytes", bytes_takeaway(res)),
        "",
        picture("results-replay", replay_takeaway(res)),
        "",
    ]
    if res.ids({"reconcile"}):
        recs = res.ids({"reconcile"})
        parts += [
            f"**Reconcile.** {reconcile_takeaway(res)}", "", reconcile_table(res), "",
            f"<sub>{res.provenance_line(recs, load=True)}</sub>", "",
        ]
    parts += [
        f"**Time.** {latency_takeaway(res)} Per-question times, the throughput chart, receipts, and the SQL: "
        f"[the measured page]({MEASURED_PAGE}).",
    ]
    if rest:
        parts += [
            "",
            "### When an index can't help",
            "",
            no_gain_text(res, rest),
            "",
            headline(res, rest, "Why not"),
        ]
    return "\n".join(parts)


def detail_block(res):
    qids = lookups(res)
    rows = ["### Bytes and row groups", "",
            "| Question | Full scan | DuckDB alone | Bellwether, unindexed | Bellwether, after fix | DuckDB on the plan | Same answer |",
            "|---|---:|---:|---:|---:|---:|---|"]
    for q in qids:
        cell = lambda side: (
            f"{commas(res.v(q, side, 'bytes'))} ({res.v(q, side, 'row_groups')})" if res.v(q, side, "bytes") is not None
            else "– (no index)" if side == "bellwether-fixed" else "– (no plan)"
        )
        same = res.v(q, "duckdb-on-plan", "same_answer")
        rows.append(
            f"| `{q}`: {res.queries[q]['text']} | {cell('full-scan')} | {cell('duckdb')} | {cell('bellwether-unindexed')} | "
            f"{cell('bellwether-fixed')} | {cell('duckdb-on-plan')} | "
            f"{'yes, ' + returned(res, q) if same else 'no plan' if same is None else 'NO'} |"
        )
    rows += ["", "Bytes (row groups). Full scan and Bellwether columns are exact footer sums; DuckDB columns are its profiler's "
             "`total_bytes_read` and `total_row_groups_scanned`, which include footer reads.", "",
             "### `why` → `fix`: predicted against realized", "",
             "| Question | `why` recommended | Key | Predicted | Realized (fix's replay after apply) | Planned (lookup after fix) | `fix` | Receipt |",
             "|---|---|---|---:|---:|---:|---|---|"]
    for q in qids:
        pred = res.v(q, "bellwether-predicted", "bytes")
        real = res.v(q, "bellwether-realized", "bytes")
        fixed = res.v(q, "bellwether-fixed", "bytes")
        rec = res.v(q, "bellwether", "receipt")
        rows.append(
            f"| `{q}` | `{res.v(q, 'bellwether', 'recommended')}` | `{res.v(q, 'bellwether', 'key')}` | "
            f"{commas(pred) if pred is not None else 'not priced'} | {commas(real) if real is not None else '–'} | "
            f"{commas(fixed) if fixed is not None else '– (no index)'} | {outcome(res, q)} | "
            f"{'`' + rec + '`' if rec else '–'} |"
        )
    rows += ["", "Receipt hashes identify the records this run wrote; a rerun writes new ones (each record carries its "
             "creation time), so they are marked `identity`, not `exact`.", "",
             "### Time", "",
             "| Question | DuckDB alone | `why` | `fix` | Lookup plan | DuckDB on the plan | Plan + DuckDB on the plan |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for q in qids:
        ms = lambda side: human_ms(res.v(q, side, "wall_ms")) if res.v(q, side, "wall_ms") is not None else "–"
        rows.append(
            f"| `{q}` | {ms('duckdb')} | {ms('bellwether-why')} | {ms('bellwether-fix')} | {ms('bellwether-plan')} | "
            f"{ms('duckdb-on-plan')} | {ms('bellwether+duckdb')} |"
        )
    rows += ["", "Median of 3 warm runs for DuckDB and the lookup; `why` and `fix` once each (they write records). "
             "`–`: not run (no index, or no `fix`). " + res.provenance_line(qids, load=True), "",
             "### Reconcile", "",
             "| Run | Rows (both sides) | Bellwether | Differing keys | Reported key | Passes | Pass | Certificate | DuckDB EXCEPT ALL | Differing rows |",
             "|---|---:|---:|---:|---|---:|---|---|---:|---:|"]
    for q in res.ids({"reconcile"}):
        key = res.v(q, "bellwether", "differing_key")
        rows.append(
            f"| `{q}`: {res.queries[q]['text']} | {commas(res.v(q, 'bellwether', 'rows_compared'))} | "
            f"{human_ms(res.v(q, 'bellwether', 'wall_ms'))}, {human_rate(res.v(q, 'bellwether', 'rows_per_s'))} | "
            f"{res.v(q, 'bellwether', 'differing_keys')} | {'`' + key + '`' if key else '–'} | {res.v(q, 'bellwether', 'passes')} | "
            f"{'yes' if res.v(q, 'bellwether', 'pass') else 'no'} | `{res.v(q, 'bellwether', 'certificate')}` | "
            f"{human_ms(res.v(q, 'duckdb', 'wall_ms'))}, {human_rate(res.v(q, 'duckdb', 'rows_per_s'))} | "
            f"{res.v(q, 'duckdb', 'differing_rows')} |"
        )
    rows += ["", "### Datasets", "", "| Dataset | Files | Row groups | Rows | Bytes | Column chunks with min/max statistics |",
             "|---|---:|---:|---:|---:|---:|"]
    for ds, meta in res.doc.get("datasets", {}).items():
        if "bytes" in meta:
            rows.append(
                f"| {meta['label']} | {commas(meta['files'])} | {commas(meta['row_groups'])} | {commas(meta['rows'])} | "
                f"{commas(meta['bytes'])} | {commas(meta.get('column_chunks_with_minmax', 0))} of {commas(meta.get('column_chunks', 0))} |"
            )
    rows += ["", "### The questions as SQL", "", "`<work>` is the script's scratch directory (`$SHOWCASE_DIR/work`).", ""]
    for q in res.ids():
        m = res.queries[q]
        rows.append(f"- `{q}` ({m.get('keys', '')}): `{m['sql']}`")
    rows += ["", f"<sub>{res.provenance_line()}</sub>"]
    return "\n".join(rows)


MARKER = re.compile(r"<!-- results:([a-z0-9-]+):begin -->.*?<!-- results:\1:end -->", re.S)


def replace_blocks(text, blocks):
    def sub(m):
        name = m.group(1)
        if name not in blocks:
            return m.group(0)
        return f"<!-- results:{name}:begin -->\n{blocks[name]}\n<!-- results:{name}:end -->"

    return MARKER.sub(sub, text)


# ---- main -----------------------------------------------------------------------
def read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def outputs():
    """Every file the results render, and every marker that names no source."""
    files, blocks = {}, {}
    for path in sorted(glob.glob(os.path.join(RESULTS, "*.json"))):
        doc = json.loads(read(path))
        source = doc.get("source") or os.path.splitext(os.path.basename(path))[0]
        res = Results(doc, os.path.relpath(path, REPO))
        if source == "showcase":
            for name, (ids, chart, _) in SHOWCASE_CHARTS.items():
                if not ids(res):
                    continue
                for theme in THEMES:
                    files[os.path.join(ASSETS, f"{name}-{theme}.svg")] = chart(res, theme)
            blocks["showcase-headline"] = readme_block(res)
            blocks["showcase-detail"] = detail_block(res)
            blocks["showcase-charts"] = "\n\n".join(
                picture(name, take(res), base="../assets") for name, (ids, _, take) in SHOWCASE_CHARTS.items() if ids(res)
            )
        else:
            for name, svg in generic_outputs(source, res).items():
                files[os.path.join(ASSETS, name)] = svg
            blocks[source] = generic_table(res)
    docs = [os.path.join(REPO, "README.md")] + sorted(glob.glob(os.path.join(REPO, "docs", "**", "*.md"), recursive=True))
    unknown = []
    for path in docs:
        text = read(path)
        unknown += [f"{os.path.relpath(path, REPO)}: results:{m.group(1)}" for m in MARKER.finditer(text) if m.group(1) not in blocks]
        new = replace_blocks(text, blocks)
        if new != text:
            files[path] = new
    return files, unknown


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if any output is stale")
    a = ap.parse_args()
    files, unknown = outputs()
    stale = [p for p, c in files.items() if not os.path.exists(p) or read(p) != c]
    orphans = [p for p in glob.glob(os.path.join(ASSETS, "results-*.svg")) if p not in files]
    for u in unknown:
        print(f"marker with no results file: {u} (add docs/measured/results/<source>.json or remove the marker)")
    if a.check:
        for p in stale:
            print(f"stale: {os.path.relpath(p, REPO)} (run scripts/render_charts.py)")
        for p in orphans:
            print(f"orphan: {os.path.relpath(p, REPO)} (no results file renders it; delete it)")
        if stale or orphans or unknown:
            sys.exit(1)
        print(f"{len(files)} outputs match the committed results")
        return
    for p in stale:
        with open(p, "w", encoding="utf-8") as f:
            f.write(files[p])
        print(f"wrote {os.path.relpath(p, REPO)}")
    for p in orphans:
        os.remove(p)
        print(f"removed {os.path.relpath(p, REPO)}")
    if unknown:
        sys.exit(1)


if __name__ == "__main__":
    main()
