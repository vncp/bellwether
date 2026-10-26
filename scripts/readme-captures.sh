#!/usr/bin/env bash
# Regenerates the README's terminal captures: docs/assets/readme/capture-*.txt
# (the session as printed, escapes kept) and, from them,
# docs/assets/readme/capture-*-{light,dark}.svg (scripts/render_terminal.py).
#
#   scripts/readme-captures.sh                        # build (debug) and capture
#   BELLWETHER=/path/to/bellwether scripts/readme-captures.sh   # skip the build
#
# Needs: cargo (unless BELLWETHER is set), python3, duckdb (2.x, run as
# `duckdb -no-agent`; it plants the one changed row reconcile finds).
# Every dataset is generated here by `bellwether bench` (no download, no
# production data) into $READMECAP_DIR (default /tmp/bellwether-readme),
# which is deleted and recreated; nothing else is written outside the repo.
#
# Every capture is real output of the command its `$` line shows, run in a
# pseudo-terminal (so headings are styled as a person sees them) from
# $READMECAP_DIR. Hashes and timestamps differ on every run; counts, bytes,
# and verdicts do not. CI checks only that the SVGs match the committed text
# (scripts/render_terminal.py --check); it never runs the engine.

set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
OUT=$REPO/docs/assets/readme
W=${READMECAP_DIR:-/tmp/bellwether-readme}
RENDER=$REPO/scripts/render_terminal.py

if [ -z "${BELLWETHER:-}" ]; then
  export CARGO_INCREMENTAL=0
  export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/bellwether-target-readme}
  (cd "$REPO" && cargo build -p bellwether-cli)
  BELLWETHER=$CARGO_TARGET_DIR/debug/bellwether
  KIND=${BELLWETHER_BUILD_KIND:-debug}
else
  KIND=${BELLWETHER_BUILD_KIND:-unlabeled}
fi
BW=$BELLWETHER
# The engine's sources, not this branch's docs: the commit of the newest
# change to crates/ or Cargo.lock.
ENGINE=$(git -C "$REPO" log -1 --format=%h -- crates Cargo.lock Cargo.toml)
TODAY=$(date -u +%Y-%m-%d)
BUILD="$KIND build of $ENGINE, $TODAY"

rm -rf "$W"
mkdir -p "$W"
cd "$W"

# ---- transcript helpers -----------------------------------------------------
CAP=""
start() { # start NAME TITLE CAPTION: a new transcript
  CAP=$OUT/capture-$1.txt
  printf '#! title: %s\n#! caption: %s\n' "$2" "$3" >"$CAP"
}
show() { # show ARGS...: `$ bellwether ARGS`, its output, and a nonzero exit
  local code=0
  [ "$(wc -l <"$CAP")" -gt 2 ] && printf '\n' >>"$CAP"
  printf '$ bellwether %s\n' "$*" >>"$CAP"
  python3 "$RENDER" --run "$BW" "$@" >>"$CAP" || code=$?
  [ "$code" -eq 0 ] || printf '#! exit %s\n' "$code" >>"$CAP"
  return 0
}
quiet() { "$BW" "$@" >/dev/null; } # setup the transcripts do not show
field() { # field FILE PATTERN: the first capture group of PATTERN in FILE
  python3 -c 'import re,sys; m=re.search(sys.argv[2], re.sub(r"\x1b\[[0-9;]*m", "", open(sys.argv[1]).read())); print(m.group(1) if m else sys.exit(f"no match for {sys.argv[2]} in {sys.argv[1]}"))' "$1" "$2"
}

# ---- datasets ---------------------------------------------------------------
# A small versioned ledger (entity_id, batch_id, version_ts, status, amount,
# payload) with a recorded workload, and the bitemporal market corpus.
quiet bench generate --out ledger --seed 42 --files 16 --days 4 --row-groups-per-file 4 \
  --rows-per-row-group 2000 --entities 20000
quiet bench workload --corpus ledger --seed 7 --runs 12 --p50 20 --p95 200 --max 400
quiet bench market --out market --rung small
LEDGER_DESC=$(python3 - ledger/corpus.json ledger/workload.json <<'PY'
import json, sys
c, w = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
files, rgs, rows, runs = len(c["files"]), c["row_groups"], c["rows"], w.get("runs")
runs = len(runs) if isinstance(runs, list) else runs
print(f"the bench ledger corpus (bench generate --seed 42: {files} files, {rgs:,} row groups, {rows:,} rows; "
      f"bench workload: {runs} recorded runs)")
PY
)

# ---- why: an unindexed folder and a recorded workload -----------------------
start why "bellwether why" "bellwether why on $LEDGER_DESC, nothing indexed; $BUILD."
show why ledger/data --bench-workload ledger
PROPOSAL=$(field "$OUT/capture-why.txt" 'apply it: bellwether fix \S+ --proposal ([0-9a-f]{64})')
WORKLOAD=$(field "$OUT/capture-why.txt" '--workload ([0-9a-f]{64})')

# ---- fix, then a reconcile that fails on one planted change -----------------
# A copy of the ledger with one entity's amount raised by 0.01 in every
# version, rewritten by DuckDB file for file; the other files are copies.
mkdir -p changed
cp -R ledger/data changed/
rm -rf changed/data/.bellwether
ENTITY=$(duckdb -no-agent -csv -noheader -c \
  "SELECT min(entity_id) FROM read_parquet('ledger/data/*/*.parquet', hive_partitioning=false)")
for f in $(duckdb -no-agent -csv -noheader -c \
  "SELECT DISTINCT filename FROM read_parquet('ledger/data/*/*.parquet', filename=true, hive_partitioning=false) WHERE entity_id = $ENTITY"); do
  rel=${f#ledger/}
  rm -f "changed/$rel"
  # The CAST keeps the column's declared decimal(18,2); a wider type would be
  # a schema difference, not the one changed value.
  duckdb -no-agent -c "SET TimeZone='UTC'; COPY (SELECT * REPLACE (CASE WHEN entity_id = $ENTITY
      THEN CAST(amount + 0.01 AS DECIMAL(18,2)) ELSE amount END AS amount)
    FROM read_parquet('$f', hive_partitioning=false)) TO 'changed/$rel' (FORMAT PARQUET)" >/dev/null
done
start fix "bellwether fix, then reconcile" \
  "bellwether fix with the proposal and workload the why capture printed, then reconcile against a copy with one entity's amount raised by 0.01 (planted with DuckDB); $LEDGER_DESC; $BUILD."
show fix ledger/data --proposal "$PROPOSAL" --workload "$WORKLOAD"
show reconcile ledger/data changed/data --key entity_id --latest-by version_ts --totals amount

# ---- report: the funnel behind the receipt, from the ledger -----------------
start report "bellwether report" \
  "bellwether report over the ledger after the fix capture; below it, the receipt's section of the report.html it wrote, drawn from that HTML (funnel bars and table as the report renders them); $BUILD."
show report ledger/data --html report.html
python3 - "$W/report.html" "$OUT/capture-report.html" <<'PY'
import re, sys
html = open(sys.argv[1], encoding="utf-8").read()
m = re.search(r"<h2>Receipts</h2>(<h3>.*?</table></div>)", html, re.S)
if not m:
    sys.exit("report.html has no receipt section")
open(sys.argv[2], "w", encoding="utf-8").write(m.group(1) + "\n")
PY

# ---- asof: bitemporal, valid time and known time ----------------------------
# One as-of query of the corpus's own workload: the first over 10 symbols
# (else the widest), its instants as the workload states them.
IFS=$'\t' read -r KEYS VALID KNOWN NSYM < <(python3 - market/workload/market.json <<'PY'
import json, sys
qs = [q for q in json.load(open(sys.argv[1]))["queries"] if q["kind"] == "asof"]
ten = [q for q in qs if len(q["symbols"]) == 10]
q = ten[0] if ten else max(qs, key=lambda q: len(q["symbols"]))
print("market/" + q["keys_file"], q["valid"], q["known"], len(q["symbols"]), sep="\t")
PY
)
start asof "bellwether asof --valid --known" \
  "bellwether index and asof on the bench market corpus (bench market --rung small: 50 symbols, 10 days, prints with restatements and late arrivals), one as-of query of the corpus's own workload ($NSYM symbols); $BUILD."
show index market/data --partitioning hive --key exact:symbol@ts --key range:ts --key temporal2:symbol@ts,known_at
show asof market/data --keys "$KEYS" --valid "$VALID" --known "$KNOWN"

# ---- MCP: a refusal over stdio ----------------------------------------------
start mcp "bellwether mcp (stdio)" \
  "A JSON-RPC session with bellwether mcp over stdio on $LEDGER_DESC: observe.why, then propose.key with a score smuggled in; the request as sent, the reply pretty-printed, nothing else edited; $BUILD."
python3 - "$BW" ledger/data >>"$OUT/capture-mcp.txt" <<'PY'
import json, subprocess, sys
bw, root = sys.argv[1], sys.argv[2]
p = subprocess.Popen([bw, "mcp", root], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
n = 0
def send(msg):
    p.stdin.write(json.dumps(msg) + "\n")
    p.stdin.flush()
def request(method, params):
    global n
    n += 1
    send({"jsonrpc": "2.0", "id": n, "method": method, "params": params})
    while True:
        v = json.loads(p.stdout.readline())
        if v.get("id") == n:
            return v
request("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                       "clientInfo": {"name": "readme-captures", "version": "0"}})
send({"jsonrpc": "2.0", "method": "notifications/initialized"})
why = request("tools/call", {"name": "observe.why", "arguments": {}})
evidence = json.loads(why["result"]["content"][0]["text"])["evidence"]["hash"]
call = {"name": "propose.key", "arguments": {"spec": {"kind": "exact", "columns": ["entity_id"]},
        "evidence": [evidence], "by": "coding-agent", "score": 0.99}}
print(f"$ bellwether mcp {root}")
print("#! (initialize, then observe.why, which returned evidence " + evidence[:12] + "...)")
print("#! client -> server, the line as sent")
print(json.dumps({"jsonrpc": "2.0", "id": n + 1, "method": "tools/call", "params": call}))
r = request("tools/call", call)
print("#! server -> client, pretty-printed")
print(json.dumps(r, indent=2))
p.stdin.close()
p.wait()
PY

python3 "$RENDER"
echo "captures in $OUT; scratch data in $W (delete it when done)"
