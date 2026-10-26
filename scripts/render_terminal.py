#!/usr/bin/env python3
"""Render terminal captures into SVG for the README. Never edit the outputs.

Each docs/assets/readme/capture-<name>.txt is a session transcript written by
scripts/readme-captures.sh: real command output, ANSI escapes kept. This
script draws it, deterministically, as

  docs/assets/readme/capture-<name>-{light,dark}.svg

a 1000-pixel terminal window, monospace, long lines wrapped at a word
boundary, with the capture's source line as a caption at the bottom. When a
capture-<name>.html sits beside the text (a section of a report the session
wrote), it is drawn under the output in the report's own palette.

  scripts/render_terminal.py                write every SVG
  scripts/render_terminal.py --check        exit 1 if a committed SVG differs
                                            from what its text renders (CI;
                                            needs no engine run)
  scripts/render_terminal.py --run CMD...   run CMD in a pseudo-terminal and
                                            print what it printed, escapes
                                            kept (readme-captures.sh uses it)

Transcript format, line by line:

  #! title: TEXT     the window title (once, at the top)
  #! caption: TEXT   the source line drawn under the output (once, at the top)
  #! TEXT            a note the capture script wrote (an exit code), drawn muted
  $ COMMAND          a command, drawn after a prompt
  anything else      output, drawn as printed (SGR escapes honored)

Standard library only.
"""

import argparse
import glob
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ASSETS = os.path.join(REPO, "docs", "assets", "readme")

WIDTH = 1000
PAD = 24
FONT_SIZE = 13
CHAR_W = FONT_SIZE * 0.6  # every monospace face the README can meet is close to 0.6 em
LINE_H = 18
COLS = int((WIDTH - 2 * PAD) / CHAR_W)  # 123
CONT_INDENT = 4
MONO = "ui-monospace,SFMono-Regular,'SF Mono',Menlo,Consolas,'DejaVu Sans Mono','Liberation Mono',monospace"
SANS = "-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif"

THEMES = {
    "light": {
        "window": "#f6f8fa",
        "border": "#d0d7de",
        "bar": "#eaeef2",
        "fg": "#1f2328",
        "muted": "#656d76",
        "prompt": "#1a7f37",
        "command": "#0969da",
        "dots": ("#d0d7de", "#d0d7de", "#d0d7de"),
        "ansi": ["#24292f", "#cf222e", "#1a7f37", "#9a6700", "#0969da", "#8250df", "#1b7c83", "#6e7781"],
        "bright": ["#57606a", "#a40e26", "#2da44e", "#bf8700", "#218bff", "#a475f9", "#3192aa", "#8c959f"],
    },
    "dark": {
        "window": "#161b22",
        "border": "#30363d",
        "bar": "#21262d",
        "fg": "#e6edf3",
        "muted": "#8b949e",
        "prompt": "#3fb950",
        "command": "#58a6ff",
        "dots": ("#484f58", "#484f58", "#484f58"),
        "ansi": ["#484f58", "#ff7b72", "#3fb950", "#d29922", "#58a6ff", "#bc8cff", "#39c5cf", "#b1bac4"],
        "bright": ["#6e7681", "#ffa198", "#56d364", "#e3b341", "#79c0ff", "#d2a8ff", "#56d4dd", "#ffffff"],
    },
}

CSI = re.compile(r"\x1b\[([0-9;?]*)([@-~])")
OSC = re.compile(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ---- ANSI ----------------------------------------------------------------------
def sgr(state, params):
    """Apply one SGR parameter list to a style dict (bold, dim, italic, underline, fg)."""
    codes = [int(p) if p.isdigit() else 0 for p in params.split(";")] if params else [0]
    i = 0
    while i < len(codes):
        c = codes[i]
        if c == 0:
            state.clear()
        elif c == 1:
            state["bold"] = True
        elif c == 2:
            state["dim"] = True
        elif c == 3:
            state["italic"] = True
        elif c == 4:
            state["underline"] = True
        elif c == 22:
            state.pop("bold", None)
            state.pop("dim", None)
        elif c == 23:
            state.pop("italic", None)
        elif c == 24:
            state.pop("underline", None)
        elif 30 <= c <= 37:
            state["fg"] = ("ansi", c - 30)
        elif 90 <= c <= 97:
            state["fg"] = ("bright", c - 90)
        elif c == 39:
            state.pop("fg", None)
        elif c == 38 and i + 1 < len(codes):
            if codes[i + 1] == 5 and i + 2 < len(codes):
                n = codes[i + 2]
                state["fg"] = ("ansi", n) if n < 8 else ("bright", n - 8) if n < 16 else ("rgb", xterm256(n))
                i += 2
            elif codes[i + 1] == 2 and i + 4 < len(codes):
                state["fg"] = ("rgb", "#%02x%02x%02x" % tuple(codes[i + 2:i + 5]))
                i += 4
        # Background colors and anything else are not drawn.
        elif c in (48,) and i + 1 < len(codes):
            i += 2 if codes[i + 1] == 5 else 4 if codes[i + 1] == 2 else 0
        i += 1


def xterm256(n):
    if n >= 232:
        v = 8 + (n - 232) * 10
        return "#%02x%02x%02x" % (v, v, v)
    n -= 16
    steps = [0, 95, 135, 175, 215, 255]
    return "#%02x%02x%02x" % (steps[n // 36], steps[(n // 6) % 6], steps[n % 6])


def spans(line, base=None):
    """A line with escapes as [(text, style)], escapes removed."""
    line = OSC.sub("", line)
    out, state, pos = [], dict(base or {}), 0
    for m in CSI.finditer(line):
        if m.start() > pos:
            out.append((line[pos:m.start()], dict(state)))
        if m.group(2) == "m":
            sgr(state, m.group(1))
        pos = m.end()
    if pos < len(line):
        out.append((line[pos:], dict(state)))
    # Any other control character (a bell, a stray escape) is dropped.
    return [(re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", "", t), s) for t, s in out if t], state


# ---- wrapping ------------------------------------------------------------------
def wrap(chunks, cols=COLS):
    """Wrap styled chunks to `cols` columns, breaking at the last space that
    fits; a continuation keeps the line's indent plus CONT_INDENT."""
    text = "".join(t for t, _ in chunks)
    if len(text) <= cols:
        return [chunks]
    styles = []
    for t, s in chunks:
        styles.extend([s] * len(t))
    indent = len(text) - len(text.lstrip(" ")) + CONT_INDENT
    indent = min(indent, cols // 3)
    lines, start, first = [], 0, True
    while start < len(text):
        room = cols if first else cols - indent
        if len(text) - start <= room:
            end = len(text)
        else:
            end = start + room
            # The last space that fits, unless it would leave a stub line.
            cut = text.rfind(" ", start + 1, end + 1)
            if cut > start + room // 3:
                end = cut
        prefix = [] if first else [(" " * indent, {})]
        lines.append(prefix + regroup(text, styles, range(start, end)))
        start = end
        while start < len(text) and text[start] == " ":
            start += 1
        first = False
    return lines


def regroup(text, styles, idx):
    out = []
    for i in idx:
        if out and out[-1][1] == styles[i]:
            out[-1] = (out[-1][0] + text[i], out[-1][1])
        else:
            out.append((text[i], styles[i]))
    return out


# ---- transcript ----------------------------------------------------------------
def parse(text):
    meta, rows, state = {}, [], {}
    body_started = False
    for raw in text.rstrip("\n").split("\n"):
        raw = raw.rstrip("\r")
        m = re.match(r"#! (title|caption): (.*)$", raw)
        if m and not body_started:
            meta[m.group(1)] = m.group(2).strip()
            continue
        body_started = True
        if raw.startswith("$ "):
            rows.append(("command", [(raw[2:], {})]))
            state = {}
        elif raw.startswith("#! "):
            rows.append(("note", [(raw[3:], {})]))
        else:
            chunks, state = spans(raw.rstrip(), state)
            rows.append(("out", chunks))
    for key in ("title", "caption"):
        if key not in meta:
            raise ValueError(f"missing '#! {key}: ...' line")
    return meta, rows


def color(style, th):
    fg = style.get("fg")
    if fg is None:
        return None
    kind, v = fg
    if kind == "rgb":
        return v
    return th[kind][v]


def tspan(text, style, th, default_fill=None):
    attrs = []
    fill = color(style, th) or default_fill
    if style.get("dim"):
        fill = th["muted"]
    if fill:
        attrs.append(f'fill="{fill}"')
    if style.get("bold"):
        attrs.append('font-weight="700"')
    if style.get("italic"):
        attrs.append('font-style="italic"')
    if style.get("underline"):
        attrs.append('text-decoration="underline"')
    t = esc(text)
    return f"<tspan {' '.join(attrs)}>{t}</tspan>" if attrs else t


def render(text, theme, page=None):
    """The SVG for a transcript; `page` is an HTML fragment of a report the
    session wrote, drawn under the output."""
    th = THEMES[theme]
    meta, rows = parse(text)
    lines = []  # (kind, chunks)
    for kind, chunks in rows:
        if kind == "command":
            # The prompt takes two columns.
            for i, part in enumerate(wrap(chunks, COLS - 2)):
                lines.append(("command" if i == 0 else "command-cont", part))
        else:
            for part in wrap(chunks):
                lines.append((kind, part))
    top = 40  # title bar
    body_y = top + PAD
    body_h = len(lines) * LINE_H
    page_svg, page_h = report_page(page, theme, PAD, body_y + body_h + 8, WIDTH - 2 * PAD) if page else ([], 0)
    caption_lines = wrap_sans(meta["caption"], WIDTH - 2 * PAD, 12)
    foot_y = body_y + body_h + (page_h + 16 if page else 0) + PAD - 4
    height = int(foot_y + 14 + len(caption_lines) * 17 + 12)
    title_id = "title"
    o = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {height}" width="{WIDTH}" '
        f'height="{height}" role="img" aria-labelledby="{title_id} desc">',
        f'<title id="{title_id}">{esc(meta["title"])}</title>',
        f'<desc id="desc">{esc(meta["caption"])}</desc>',
        f'<rect x="0.5" y="0.5" width="{WIDTH - 1}" height="{height - 1}" rx="10" fill="{th["window"]}" stroke="{th["border"]}"/>',
        f'<path d="M0.5 {top}V10.5a10 10 0 0 1 10-10H{WIDTH - 10.5}a10 10 0 0 1 10 10V{top}Z" fill="{th["bar"]}"/>',
        f'<line x1="0.5" y1="{top}" x2="{WIDTH - 0.5}" y2="{top}" stroke="{th["border"]}"/>',
    ]
    for i, dot in enumerate(th["dots"]):
        o.append(f'<circle cx="{22 + i * 18}" cy="{top / 2}" r="5.5" fill="{dot}"/>')
    o.append(
        f'<text x="{WIDTH / 2}" y="{top / 2 + 4.5}" text-anchor="middle" font-family="{SANS}" font-size="13" '
        f'fill="{th["muted"]}">{esc(meta["title"])}</text>'
    )
    # Runs of spaces are columns: every text keeps them (xml:space for SVG
    # renderers, white-space for browsers, which ignore an inherited xml:space).
    o.append(f'<g font-family="{MONO}" font-size="{FONT_SIZE}" fill="{th["fg"]}">')
    for n, (kind, chunks) in enumerate(lines):
        y = body_y + n * LINE_H + FONT_SIZE
        lead, chunks = strip_chunks(chunks)
        if not chunks:
            continue
        length = sum(len(t) for t, _ in chunks)
        if kind == "command":
            inner = f'<tspan fill="{th["prompt"]}">$ </tspan>' + "".join(
                tspan(t, s, th, th["command"]) for t, s in chunks
            )
            length += 2
        elif kind == "command-cont":
            # Under the command, past the prompt's two columns.
            lead += 2
            inner = "".join(tspan(t, s, th, th["command"]) for t, s in chunks)
        elif kind == "note":
            inner = "".join(tspan(t, s, th, th["muted"]) for t, s in chunks)
        else:
            inner = "".join(tspan(t, s, th) for t, s in chunks)
        # Leading spaces become the x offset and trailing ones are dropped, so
        # textLength pins exactly the visible columns in any monospace face.
        o.append(
            f'<text x="{PAD + lead * CHAR_W:.1f}" y="{y}" textLength="{length * CHAR_W:.1f}" '
            f'lengthAdjust="spacing" xml:space="preserve" style="white-space:pre">{inner}</text>'
        )
    o.append("</g>")
    o.extend(page_svg)
    o.append(f'<line x1="{PAD}" y1="{foot_y}" x2="{WIDTH - PAD}" y2="{foot_y}" stroke="{th["border"]}"/>')
    for i, cl in enumerate(caption_lines):
        o.append(
            f'<text x="{PAD}" y="{foot_y + 22 + i * 17}" font-family="{SANS}" font-size="12" font-style="italic" '
            f'fill="{th["muted"]}">{esc(cl)}</text>'
        )
    o.append("</svg>")
    return "\n".join(o) + "\n"


# ---- a report page under the output ---------------------------------------------
# The report's own palette, light and dark (crates/bellwether/src/report.rs STYLE).
REPORT = {
    "light": {"bg": "#ffffff", "fg": "#1d2330", "muted": "#5b6475", "rule": "#d9dde5", "now": "#b54a3c",
              "fix": "#2f6f9f", "ok": "#2e7d4f", "bad": "#b54a3c"},
    "dark": {"bg": "#14171d", "fg": "#e4e7ee", "muted": "#9aa3b5", "rule": "#2c323d", "now": "#e07a6a",
             "fix": "#6fb0e0", "ok": "#6cc08e", "bad": "#e07a6a"},
}


def unescape(s):
    return (s.replace("&rarr;", "→").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
            .replace("&#39;", "'").replace("&amp;", "&"))


def inline(html, rp, fill, size):
    """Inline HTML (text, <code>, <span class=pass|fail>) as tspans, and its
    plain text."""
    out, plain = [], []
    for m in re.finditer(r'<code>(.*?)</code>|<span class="(pass|fail)">(.*?)</span>|([^<]+)|<[^>]*>', html):
        if m.group(1) is not None:
            t = unescape(m.group(1))
            out.append(f'<tspan font-family="{MONO}" font-size="{size - 1}">{esc(t)}</tspan>')
        elif m.group(2):
            t = unescape(m.group(3))
            out.append(f'<tspan fill="{rp["ok" if m.group(2) == "pass" else "bad"]}" font-weight="600">{esc(t)}</tspan>')
        elif m.group(4):
            t = unescape(m.group(4))
            out.append(esc(t))
        else:
            continue
        plain.append(t)
    return "".join(out), "".join(plain)


def report_page(html, theme, x, y, w):
    """One receipt's section of report.html (its heading, paragraphs, funnel
    SVG, and table), drawn as the report draws it, in the report's palette.
    Returns (elements, height)."""
    rp, th = REPORT[theme], THEMES[theme]
    o = []
    cx, cy = x + 20, y + 40  # content origin, under the page's tab
    tab = "report.html"
    parts = re.findall(r"<h3>.*?</h3>|<p[^>]*>.*?</p>|<svg.*?</svg>|<table>.*?</table>", html, re.S)
    for part in parts:
        if part.startswith("<h3>"):
            inner, _ = inline(part[4:-5], rp, rp["fg"], 15)
            o.append(f'<text x="{cx}" y="{cy + 15}" font-family="{SANS}" font-size="15" font-weight="600" '
                     f'fill="{rp["fg"]}">{inner}</text>')
            cy += 28
        elif part.startswith("<p"):
            muted = 'class="muted"' in part.split(">", 1)[0]
            body = re.sub(r"^<p[^>]*>|</p>$", "", part)
            inner, plain = inline(body, rp, rp["muted"] if muted else rp["fg"], 13)
            if len(plain) * 13 * 0.52 > w - 40:
                raise ValueError(f"report paragraph too long for one line: {plain!r}")
            o.append(f'<text x="{cx}" y="{cy + 13}" font-family="{SANS}" font-size="13" '
                     f'fill="{rp["muted"] if muted else rp["fg"]}">{inner}</text>')
            cy += 22
        elif part.startswith("<svg"):
            vb = re.search(r'viewBox="0 0 ([\d.]+) ([\d.]+)"', part)
            sw, sh = float(vb.group(1)), float(vb.group(2))
            body = re.sub(r"^<svg[^>]*>|</svg>$", "", part)
            for var in ("now", "fix", "muted", "fg"):
                body = body.replace(f"var(--{var})", rp[var])
            # The report styles its chart text with CSS (svg text: fg, 12px sans).
            body = body.replace("<text ", f'<text fill="{rp["fg"]}" font-family="{SANS}" font-size="12" ')
            o.append(f'<svg x="{cx}" y="{cy + 4}" width="{sw:g}" height="{sh:g}" viewBox="0 0 {sw:g} {sh:g}">'
                     f"{body}</svg>")
            cy += sh + 8
        else:
            rows = re.findall(r"<tr>(.*?)</tr>", part, re.S)
            cols = [x + 20, x + w - 460, x + w - 300, x + w - 40]
            for i, row in enumerate(rows):
                cells = [unescape(re.sub(r"<[^>]+>", "", c)) for c in re.findall(r"<t[hd]>(.*?)</t[hd]>", row, re.S)]
                weight = ' font-weight="600"' if i == 0 else ""
                for j, c in enumerate(cells[:4]):
                    anchor = "" if j == 0 else ' text-anchor="end"'
                    o.append(f'<text x="{cols[j]}" y="{cy + 15}" font-family="{SANS}" font-size="13" '
                             f'fill="{rp["fg"]}"{anchor}{weight}>{esc(c)}</text>')
                cy += 24
                o.append(f'<line x1="{x + 20}" y1="{cy - 2}" x2="{x + w - 20}" y2="{cy - 2}" stroke="{rp["rule"]}"/>')
            cy += 6
    h = cy - y + 12
    frame = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="{rp["bg"]}" stroke="{th["border"]}"/>',
        f'<text x="{x + 20}" y="{y + 24}" font-family="{MONO}" font-size="12" fill="{th["muted"]}">{tab}</text>',
        f'<line x1="{x}" y1="{y + 34}" x2="{x + w}" y2="{y + 34}" stroke="{th["border"]}"/>',
    ]
    return frame + o, h



def strip_chunks(chunks):
    """(leading space count, chunks without leading or trailing spaces)."""
    chunks = [c for c in chunks if c[0]]
    lead = 0
    while chunks and chunks[0][0].startswith(" "):
        t, s = chunks[0]
        stripped = t.lstrip(" ")
        lead += len(t) - len(stripped)
        chunks = ([(stripped, s)] if stripped else []) + chunks[1:]
    while chunks and chunks[-1][0].endswith(" "):
        t, s = chunks[-1]
        stripped = t.rstrip(" ")
        chunks = chunks[:-1] + ([(stripped, s)] if stripped else [])
    return lead, chunks


def wrap_sans(text, width, size):
    """Wrap caption text by an average sans advance of 0.52 em."""
    per = max(20, int(width / (size * 0.52)))
    words, lines, cur = text.split(), [], ""
    for w in words:
        if cur and len(cur) + 1 + len(w) > per:
            lines.append(cur)
            cur = w
        else:
            cur = f"{cur} {w}" if cur else w
    if cur:
        lines.append(cur)
    return lines


# ---- run -----------------------------------------------------------------------
def run_in_pty(argv):
    """Run argv with a pseudo-terminal as stdout and stderr (so the program
    styles its output as it would for a person) and print what it wrote.
    The terminal is a plain color one: an operator's NO_COLOR or TERM=dumb
    does not reach the program. Exits with its exit code."""
    import pty
    import subprocess

    leader, follower = pty.openpty()
    try:
        import fcntl
        import struct
        import termios

        fcntl.ioctl(follower, termios.TIOCSWINSZ, struct.pack("HHHH", 50, COLS, 0, 0))
    except (ImportError, OSError):
        pass
    env = {k: v for k, v in os.environ.items() if k != "NO_COLOR"}
    env["TERM"] = "xterm-256color"
    proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=follower, stderr=follower, close_fds=True,
                            env=env)
    os.close(follower)
    buf = bytearray()
    while True:
        try:
            chunk = os.read(leader, 65536)
        except OSError:  # EIO once the child closes its end (Linux)
            break
        if not chunk:
            break
        buf.extend(chunk)
    os.close(leader)
    code = proc.wait()
    sys.stdout.write(buf.decode("utf-8", "replace").replace("\r\n", "\n"))
    sys.stdout.flush()
    sys.exit(code)


# ---- main ----------------------------------------------------------------------
def outputs():
    files = {}
    for path in sorted(glob.glob(os.path.join(ASSETS, "capture-*.txt"))):
        name = os.path.splitext(os.path.basename(path))[0]
        text = open(path, encoding="utf-8").read()
        # A report the session wrote, kept beside its transcript.
        page_path = os.path.join(ASSETS, f"{name}.html")
        page = open(page_path, encoding="utf-8").read() if os.path.exists(page_path) else None
        for theme in THEMES:
            try:
                files[os.path.join(ASSETS, f"{name}-{theme}.svg")] = render(text, theme, page)
            except ValueError as e:
                sys.exit(f"{os.path.relpath(path, REPO)}: {e}")
    return files


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--check", action="store_true", help="exit 1 if any SVG is stale")
    ap.add_argument("--run", nargs=argparse.REMAINDER, metavar="CMD", help="run CMD in a pseudo-terminal")
    a = ap.parse_args()
    if a.run:
        run_in_pty(a.run)
    files = outputs()
    stale = [p for p, c in files.items() if not os.path.exists(p) or open(p, encoding="utf-8").read() != c]
    orphans = [p for p in glob.glob(os.path.join(ASSETS, "capture-*.svg")) if p not in files]
    if a.check:
        for p in stale:
            print(f"stale: {os.path.relpath(p, REPO)} (run scripts/render_terminal.py)")
        for p in orphans:
            print(f"orphan: {os.path.relpath(p, REPO)} (no capture text renders it; delete it)")
        if stale or orphans:
            sys.exit(1)
        print(f"{len(files)} capture SVGs match their text")
        return
    for p in stale:
        with open(p, "w", encoding="utf-8") as f:
            f.write(files[p])
        print(f"wrote {os.path.relpath(p, REPO)}")
    for p in orphans:
        os.remove(p)
        print(f"removed {os.path.relpath(p, REPO)}")


if __name__ == "__main__":
    main()
