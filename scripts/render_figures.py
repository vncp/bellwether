#!/usr/bin/env python3
"""Draw the README's figures in GitHub's light and dark themes. Never edit the outputs.

Writes, deterministically, from the definitions below:

  docs/assets/readme/wordmark-{light,dark}.svg       the name and its mark
  docs/assets/readme/loop-{light,dark}.svg           the loop and the trust boundary
  docs/assets/readme/three-steps-{light,dark}.svg    ask, fix, proof
  docs/assets/readme/architecture-{light,dark}.svg   surfaces, crates, storage

  scripts/render_figures.py           write every figure
  scripts/render_figures.py --check   exit 1 if a committed figure differs (CI)

Every label is checked against the width of the box it sits in (an
estimate of the face's advance, on the wide side), so an edit that would
overflow fails here instead of on GitHub. Standard library only.
"""

import argparse
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS = os.path.join(REPO, "docs", "assets", "readme")

SANS = "-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif"
MONO = "ui-monospace,SFMono-Regular,'SF Mono',Menlo,Consolas,'DejaVu Sans Mono',monospace"

THEMES = {
    "light": {
        "panel": "#f6f8fa", "panel_line": "#d0d7de", "box": "#ffffff", "box_line": "#d0d7de",
        "inner": "#f6f8fa", "fg": "#1f2328", "muted": "#656d76", "faint": "#8c959f",
        "agent": "#0969da", "engine": "#1a7f37", "boundary": "#cf222e", "proof": "#9a6700",
        "on_accent": "#ffffff",
    },
    "dark": {
        "panel": "#0d1117", "panel_line": "#30363d", "box": "#161b22", "box_line": "#30363d",
        "inner": "#0d1117", "fg": "#e6edf3", "muted": "#8b949e", "faint": "#6e7681",
        "agent": "#58a6ff", "engine": "#3fb950", "boundary": "#ff7b72", "proof": "#d29922",
        "on_accent": "#0d1117",
    },
}


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ---- text width (an over-estimate, so a pass here fits on GitHub) ---------------
NARROW = set("ijlt.,:;!|'()[]· ")
WIDE = set("mwMW@%")


def text_width(s, size, mono=False, bold=False, spacing=0.0):
    if mono:
        return len(s) * size * 0.62
    w = 0.0
    for c in s:
        if c in NARROW:
            w += 0.32
        elif c in WIDE:
            w += 0.9
        elif c.isupper() or c.isdigit():
            w += 0.68
        else:
            w += 0.56
    w *= size * (1.06 if bold else 1.0)
    return w + spacing * size * len(s)


class Svg:
    def __init__(self, w, h, title, desc, theme):
        self.w, self.h, self.th = w, h, THEMES[theme]
        self.o = [
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
            f'role="img" aria-labelledby="title desc">',
            f'<title id="title">{esc(title)}</title>',
            f'<desc id="desc">{esc(desc)}</desc>',
        ]

    def c(self, key):
        return self.th.get(key, key)

    def add(self, s):
        self.o.append(s)

    def rect(self, x, y, w, h, fill, stroke=None, rx=12, sw=1):
        s = f' stroke="{self.c(stroke)}" stroke-width="{sw}"' if stroke else ""
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{self.c(fill)}"{s}/>')

    def line(self, x1, y1, x2, y2, stroke, sw=1, dash=None, arrow=False):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        m = ' marker-end="url(#arrow)"' if arrow else ""
        self.add(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{self.c(stroke)}" stroke-width="{sw}"{d}{m}/>')

    def path(self, d, stroke, sw=1.5, arrow=False):
        m = ' marker-end="url(#arrow)"' if arrow else ""
        self.add(f'<path d="{d}" fill="none" stroke="{self.c(stroke)}" stroke-width="{sw}"{m}/>')

    def text(self, x, y, s, size=14, fill="fg", weight=None, anchor=None, mono=False, spacing=None,
             italic=False, room=None):
        """A label; `room` is the width it must fit in (checked)."""
        bold = weight is not None and int(weight) >= 600
        if room is not None:
            need = text_width(s, size, mono, bold, spacing or 0.0)
            if need > room:
                raise SystemExit(f"label {s!r} needs ~{need:.0f}px, has {room}px")
        a = [f'x="{x}"', f'y="{y}"', f'font-size="{size}"', f'fill="{self.c(fill)}"']
        if weight:
            a.append(f'font-weight="{weight}"')
        if anchor:
            a.append(f'text-anchor="{anchor}"')
        if mono:
            a.append(f'font-family="{MONO}"')
        if spacing:
            a.append(f'letter-spacing="{spacing}em"')
        if italic:
            a.append('font-style="italic"')
        self.add(f"<text {' '.join(a)}>{esc(s)}</text>")

    def arrow_marker(self, color):
        self.add(
            '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
            f'orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="{self.c(color)}"/></marker></defs>'
        )

    def done(self):
        return "\n".join(self.o) + "\n</svg>\n"


# ---- the wordmark ---------------------------------------------------------------
def wordmark(theme):
    s = Svg(380, 80, "Bellwether", "The Bellwether wordmark: a bell in a rounded square, then the name.", theme)
    s.rect(4, 8, 64, 64, "fg", rx=12)
    bell = s.c("box")
    s.add(f'<circle cx="36" cy="21" r="3.2" fill="{bell}"/>')
    s.add(
        f'<path d="M36 24C28.5 24 25 29.5 25 37V45L20.5 51H51.5L47 45V37C47 29.5 43.5 24 36 24Z" fill="{bell}"/>'
    )
    s.add(f'<circle cx="36" cy="56" r="3.8" fill="{bell}"/>')
    s.add(
        f'<text x="88" y="59" font-family="{SANS}" font-size="50" font-weight="600" letter-spacing="-0.01em" '
        f'fill="{s.c("fg")}">Bellwether</text>'
    )
    return s.done()


# ---- the loop -------------------------------------------------------------------
def card_rows(s, x, y, w, header, sub, rows, top, step, verb_w, verb_size=19):
    """A card with a header, a subtitle, and rows of (verb, line, line)."""
    s.text(x + 26, y + 36, header, 12, "muted", 600, spacing=0.12, room=w - 52)
    s.text(x + 26, y + 59, sub, 13, "faint", room=w - 52)
    s.line(x + 26, y + 78, x + w - 26, y + 78, "box_line")
    for i, (verb, *lines) in enumerate(rows):
        ry = top + i * step
        s.text(x + 26, ry, verb, verb_size, "fg", 600, room=verb_w - 8)
        for j, ln in enumerate(lines):
            s.text(x + 26 + verb_w, ry - (len(lines) - 1) * 9 + j * 19, ln, 14, "muted", room=w - 52 - verb_w)
        if i < len(rows) - 1:
            s.line(x + 26, ry + step / 2 + 4, x + w - 26, ry + step / 2 + 4, "box_line")


def loop(theme):
    s = Svg(
        1100, 560, "The Bellwether loop",
        "An agent or a person may observe and propose; only the engine may score, verify, apply, and attest. "
        "A trust boundary enforced by record types separates them: a proposal has no score field, and a receipt "
        "cannot be written without a passing certificate. Hashes cross to the engine; evidence, replays, "
        "certificates, receipts, and refusals with one next step come back. The engine reads your files' "
        "statistics and writes only its index beside them.",
        theme,
    )
    s.arrow_marker("muted")
    s.add(f'<g font-family="{SANS}">')
    s.rect(0.5, 0.5, 1099, 559, "panel", "panel_line", rx=16)
    # The agent's side.
    s.rect(30, 56, 420, 344, "box", "box_line")
    card_rows(
        s, 30, 56, 420, "THE AGENT, OR YOU", "any MCP client · the bellwether CLI",
        [
            ("observe", "why · profile · locate · asof", "read-only, from cache → Evidence"),
            ("propose", "a key, a layout, a rewrite", "no score field exists → Proposal"),
        ],
        top=200, step=120, verb_w=118,
    )
    # The engine's side.
    s.rect(650, 56, 420, 344, "box", "box_line")
    card_rows(
        s, 650, 56, 420, "ONLY THE ENGINE", "one binary · never calls a model",
        [
            ("score", "exact replay of recorded keys → Replay"),
            ("verify", "reconcile: same rows? → Certificate"),
            ("apply", "only with a passing certificate"),
            ("attest", "hash-links the chain → Receipt"),
        ],
        top=170, step=62, verb_w=92, verb_size=17,
    )
    s.text(240, 430, "may observe and propose; may say anything", 13, "faint", anchor="middle", room=400)
    s.text(840, 430, "the only writer of the index and the ledger", 13, "faint", anchor="middle", room=400)
    # The boundary.
    s.line(550, 50, 550, 410, "boundary", 2, dash="6 6")
    s.rect(466, 24, 168, 24, "panel", rx=12)
    s.text(550, 41, "TRUST BOUNDARY", 12, "boundary", 600, anchor="middle", spacing=0.12, room=164)
    s.text(550, 430, "record types, not prompts", 13, "boundary", anchor="middle", room=200)
    s.rect(458, 158, 184, 38, "panel", rx=0)
    s.text(550, 174, "Evidence and Proposal", 13, "muted", anchor="middle", room=180)
    s.text(550, 191, "hashes; nothing scored", 13, "muted", anchor="middle", room=180)
    s.line(458, 208, 640, 208, "muted", 1.5, arrow=True)
    s.line(642, 290, 460, 290, "muted", 1.5, arrow=True)
    s.rect(454, 302, 192, 38, "panel", rx=0)
    s.text(550, 318, "Replays, Certificates,", 13, "muted", anchor="middle", room=190)
    s.text(550, 335, "Receipts; refusals + next step", 13, "muted", anchor="middle", room=190)
    # Your files.
    s.rect(380, 470, 340, 66, "box", "muted")
    s.text(550, 498, "Your folder of files", 17, "fg", 600, anchor="middle", room=320)
    s.text(550, 520, "Parquet · CSV · JSON lines, local or object store", 13, "muted", anchor="middle", room=320)
    s.path("M1010 402C1010 470 990 503 726 503", "muted", arrow=True)
    s.text(744, 530, "reads footers and statistics;", 13, "muted", room=240)
    s.text(744, 547, "writes only its index, beside them", 13, "muted", room=240)
    s.add("</g>")
    return s.done()


# ---- three steps ----------------------------------------------------------------
def three_steps(theme):
    s = Svg(
        1100, 260, "Bellwether in three steps",
        "Three steps. 1 Ask: bellwether why ./data/ says which column is a real key and exactly how many bytes "
        "each index would save on your recorded queries. 2 Fix: bellwether fix ./data/ --proposal … scores, "
        "verifies the same rows come back, builds the index, and only then applies. 3 Proof: a receipt and an "
        "index; a hash-linked receipt a reviewer or CI can check, and lookups in milliseconds over plain files.",
        theme,
    )
    s.arrow_marker("muted")
    s.add(f'<g font-family="{SANS}">')
    s.rect(0.5, 0.5, 1099, 259, "panel", "panel_line", rx=16)
    steps = [
        ("Ask", "bellwether why ./data/", "agent",
         ["Which column is a real key, and", "exactly how many bytes each index", "would save on your recorded queries."]),
        ("Fix", "bellwether fix ./data/ --proposal …", "proof",
         ["Scores, verifies the same rows come", "back, builds the index, and only", "then applies."]),
        ("Proof", "receipt + index", "engine",
         ["A hash-linked receipt a reviewer or", "CI can check, and lookups in", "milliseconds over plain files."]),
    ]
    w, gap, x0 = 316, 46, 30
    for i, (name, cmd, accent, lines) in enumerate(steps):
        x = x0 + i * (w + gap)
        s.rect(x, 30, w, 200, "box", accent, sw=2)
        s.add(f'<circle cx="{x + 32}" cy="66" r="15" fill="{s.c(accent)}"/>')
        s.text(x + 32, 71, str(i + 1), 15, "on_accent", 700, anchor="middle")
        s.text(x + 58, 73, name, 20, "fg", 600, room=w - 80)
        s.text(x + 24, 110, cmd, 12.5, accent, mono=True, room=w - 40)
        for j, ln in enumerate(lines):
            s.text(x + 24, 142 + j * 20, ln, 14, "muted", room=w - 40)
        if i < len(steps) - 1:
            s.line(x + w + 6, 130, x + w + gap - 6, 130, "muted", 2, arrow=True)
    s.add("</g>")
    return s.done()


# ---- architecture ---------------------------------------------------------------
def architecture(theme):
    s = Svg(
        1100, 600, "Bellwether architecture",
        "Who drives it: you in a terminal, a coding agent over MCP, CI or a script, and the benchmark, all through "
        "one binary. Inside the Rust engine, five layers with dependencies pointing one way: bellwether-cli and "
        "bellwether-mcp (arguments and envelopes only), the bellwether facade (fix runs score, verify, apply, "
        "attest), profile, replay, verify and build, then core, source, store and ledger, then bellwether-types, "
        "the contract. The engine reads statistics from your files and writes segments, a manifest, a ledger and "
        "a cache beside them, on local disk or object storage.",
        theme,
    )
    s.arrow_marker("muted")
    s.add(f'<g font-family="{SANS}">')
    s.rect(0.5, 0.5, 1099, 599, "panel", "panel_line", rx=16)

    side_w, eng_x, eng_w, right_x = 270, 340, 420, 800

    def side_card(x, header, sub, rows, foot):
        room = side_w - 44
        s.rect(x, 56, side_w, 480, "box", "box_line")
        s.text(x + 22, 92, header, 12, "muted", 600, spacing=0.12, room=room)
        s.text(x + 22, 115, sub, 13, "faint", room=room)
        s.line(x + 22, 134, x + side_w - 22, 134, "box_line")
        for i, (name, detail, mono) in enumerate(rows):
            y = 168 + i * 72
            s.text(x + 22, y, name, 14 if mono else 16, "fg", 700 if mono else 600, mono=mono, room=room)
            s.text(x + 22, y + 21, detail, 13, "muted", room=room)
            if i < len(rows) - 1:
                s.line(x + 22, y + 40, x + side_w - 22, y + 40, "box_line")
        s.text(x + side_w / 2, 566, foot, 13, "faint", anchor="middle", room=side_w)

    side_card(
        30, "WHO DRIVES IT", "one binary, every surface",
        [
            ("You, in a terminal", "bellwether <verb> · --json", False),
            ("A coding agent", "bellwether mcp · tiered tools", False),
            ("CI or a script", "exit codes 0–4 · one JSON line", False),
            ("The benchmark", "bellwether bench · corpora, SLOs", False),
            ("Both agree", "--json is the MCP result (ADR 010)", False),
        ],
        "no surface holds engine logic",
    )
    side_card(
        right_x, "YOUR STORAGE", "local disk · S3 · Azure · GCS",
        [
            ("your files", "Parquet · CSV · JSON lines, as-is", True),
            ("segments/", "immutable, named by content hash", True),
            ("manifest.json", "advanced by conditional PUT", True),
            ("ledger/", "append-only, hash-named records", True),
            ("cache/", "rebuildable", True),
        ],
        "the data is never converted",
    )
    # The engine.
    mid = eng_x + eng_w / 2
    s.rect(eng_x, 56, eng_w, 480, "box", "box_line")
    s.text(eng_x + 24, 92, "RUST ENGINE", 12, "muted", 600, spacing=0.12, room=eng_w - 48)
    s.text(eng_x + 24, 115, "thirteen crates; imports point down", 13, "faint", room=eng_w - 48)
    s.line(eng_x + 24, 134, eng_x + eng_w - 24, 134, "box_line")
    layers = [
        ("bellwether-cli · bellwether-mcp", "arguments and envelopes; no engine logic"),
        ("bellwether", "the facade: score → verify → apply → attest"),
        ("profile · replay · verify · build", "fanout · exact cost · certificates · segments"),
        ("core · source · store · ledger", "keys · footers · commits · records"),
        ("bellwether-types", "the contract: records, traits, format"),
    ]
    for i, (name, detail) in enumerate(layers):
        y = 150 + i * 74
        s.rect(eng_x + 24, y, eng_w - 48, 58, "inner", "box_line", rx=8)
        s.text(eng_x + 42, y + 25, name, 14, "fg", 700, mono=True, room=eng_w - 84)
        s.text(eng_x + 42, y + 45, detail, 13, "muted", room=eng_w - 84)
        if i < len(layers) - 1:
            s.line(mid, y + 59, mid, y + 72, "muted", 1.5, arrow=True)
    s.text(mid, 566, "dependencies point one way: dev/check-layering.py", 13, "faint", anchor="middle",
           room=eng_w)
    # Between the cards.
    left_gap = (30 + side_w, eng_x)
    s.line(left_gap[0] + 4, 300, left_gap[1] - 4, 300, "agent", 2, arrow=True)
    s.text(sum(left_gap) / 2, 288, "calls", 12, "agent", anchor="middle", room=left_gap[1] - left_gap[0])
    right_gap = (eng_x + eng_w, right_x)
    s.line(right_gap[0] + 4, 280, right_gap[1] - 4, 280, "muted", 1.5, arrow=True)
    s.text(sum(right_gap) / 2, 268, "writes", 12, "muted", anchor="middle", room=right_gap[1] - right_gap[0])
    s.line(right_gap[1] - 4, 330, right_gap[0] + 4, 330, "muted", 1.5, arrow=True)
    s.text(sum(right_gap) / 2, 350, "reads", 12, "muted", anchor="middle", room=right_gap[1] - right_gap[0])
    s.add("</g>")
    return s.done()


FIGURES = {"wordmark": wordmark, "loop": loop, "three-steps": three_steps, "architecture": architecture}


def outputs():
    return {
        os.path.join(ASSETS, f"{name}-{theme}.svg"): fn(theme)
        for name, fn in FIGURES.items()
        for theme in THEMES
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if any figure is stale")
    a = ap.parse_args()
    files = outputs()
    stale = [p for p, c in files.items() if not os.path.exists(p) or open(p, encoding="utf-8").read() != c]
    if a.check:
        for p in stale:
            print(f"stale: {os.path.relpath(p, REPO)} (run scripts/render_figures.py)")
        if stale:
            sys.exit(1)
        print(f"{len(files)} figures match their definitions")
        return
    os.makedirs(ASSETS, exist_ok=True)
    for p in stale:
        with open(p, "w", encoding="utf-8") as f:
            f.write(files[p])
        print(f"wrote {os.path.relpath(p, REPO)}")


if __name__ == "__main__":
    main()
