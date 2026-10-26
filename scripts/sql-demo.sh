#!/usr/bin/env bash
# Tables as sources (ADR 027), measured on the data the real-data run uses
# (scripts/realdata.sh): six months of NYC yellow-taxi trips, loaded from
# their Parquet files into a DuckDB table and a SQLite table, then
#
#   1. reconcile each table against the Parquet files (and, as the baseline,
#      the files against a DuckDB rewrite of them, as realdata.sh does);
#   2. reconcile a copy of each table with three planted differences (one
#      changed tip_amount, one deleted row, one added row) against the files;
#   3. `why` the realdata.sh pickup-zone lookups over each table (every cost
#      measured) and over the files (exact footer sums), `locate` them on the
#      indexed files, and run the same lookup inside DuckDB for its own
#      rows scanned.
#
# and writes every number to docs/measured/results/sql.json (one row per
# dataset, query, side, metric; schema in docs/measured/results/README.md).
#
#   TAXI_DIR=<folder of yellow_tripdata_2024-0[1-6].parquet> scripts/sql-demo.sh
#   SQL_DEMO_DIR=/scratch BELLWETHER=/path/to/bellwether scripts/sql-demo.sh
#
# Without TAXI_DIR the six months are downloaded (about 350 MB) into
# $SQL_DEMO_DIR/dl. Needs: duckdb 2.x (run as `duckdb -no-agent`, with its
# sqlite extension, installed on first use), libduckdb (found by bellwether in
# the usual install directories, or BELLWETHER_DUCKDB_DRIVER), python3, perl,
# cargo. Writes about 6 GB under $SQL_DEMO_DIR; deletes nothing.

set -uo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
R=${SQL_DEMO_DIR:-${TMPDIR:-/tmp}/bellwether-sql-demo}
W=$R/work
OUT=$R/out
RESULTS=${RESULTS:-$REPO/docs/measured/results/sql.json}
MONTHS="01 02 03 04 05 06"
UNITS=${UNITS:-24}
BUDGET=${BUDGET:-8G}
K=VendorID,tpep_pickup_datetime,PULocationID,fare_amount
mkdir -p "$W" "$OUT"
T=$OUT/transcript.txt
: >"$T"

say() { printf '%s\n' "$*" | tee -a "$T"; }
now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
duck() { duckdb -no-agent -csv -noheader "$@"; }

free_gb=$(df -Pk / | awk 'NR==2 {print int($4 / 1048576)}')
if [ "$free_gb" -lt 12 ]; then
  echo "only ${free_gb} GB free on /; stopping (need 12)" >&2
  exit 1
fi

if [ -z "${BELLWETHER:-}" ]; then
  export CARGO_INCREMENTAL=0
  export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/bellwether-target-sql-demo}
  (cd "$REPO" && cargo build --release -p bellwether-cli) || exit 1
  BELLWETHER=$CARGO_TARGET_DIR/release/bellwether
fi
BW=$BELLWETHER
COMMIT=$(git -C "$REPO" rev-parse --short HEAD)
DUCK_VERSION=$(duckdb -no-agent --version | awk '{print $1, $3}')

# ---- data --------------------------------------------------------------------
if [ -z "${TAXI_DIR:-}" ]; then
  TAXI_DIR=$R/dl/taxi
  mkdir -p "$TAXI_DIR"
  for m in $MONTHS; do
    f=$TAXI_DIR/yellow_tripdata_2024-$m.parquet
    [ -s "$f" ] || curl -sSfL --retry 3 -o "$f" \
      "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-$m.parquet" || exit 1
  done
fi
TAXI=$(cd "$TAXI_DIR" && pwd)
cd "$W" || exit 1
rm -rf taxi_duck keys ledger-* idx-* taxi.duckdb taxi.sqlite planted.duckdb planted.sqlite
mkdir -p taxi_duck keys
say "data: $TAXI ($(duck -c "SELECT count(*) FROM read_parquet('$TAXI/*.parquet')") rows)"
for m in $MONTHS; do
  f=yellow_tripdata_2024-$m.parquet
  duck -c "COPY (SELECT * FROM read_parquet('$TAXI/$f')) TO 'taxi_duck/$f' (FORMAT PARQUET)" >/dev/null
done
# realdata.sh's pickup-zone key file: every zone, chosen by a seeded hash.
duck -c "COPY (SELECT PULocationID FROM (SELECT DISTINCT PULocationID FROM read_parquet('$TAXI/*.parquet')
  WHERE PULocationID IS NOT NULL) ORDER BY hash(PULocationID, 42), PULocationID LIMIT 1000)
  TO 'keys/taxi_pu.parquet' (FORMAT PARQUET)" >/dev/null

# The tables: the files' rows as they are, one table per engine. SQLite gets an
# index on the range column, as a table cut by pickup time would have.
t0=$(now)
duck taxi.duckdb -c "CREATE TABLE trips AS SELECT * FROM read_parquet('$TAXI/*.parquet')" >/dev/null || exit 1
say "loaded taxi.duckdb in $(perl -e 'printf "%.1f", $ARGV[1]-$ARGV[0]' "$t0" "$(now)") s"
t0=$(now)
# Declared types as the files hold them: DuckDB's SQLite writer would
# declare the timestamps VARCHAR.
sqlite3 taxi.sqlite "CREATE TABLE trips (VendorID INTEGER, tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP, passenger_count BIGINT, trip_distance DOUBLE, RatecodeID BIGINT,
  store_and_fwd_flag VARCHAR, PULocationID INTEGER, DOLocationID INTEGER, payment_type BIGINT,
  fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE,
  improvement_surcharge DOUBLE, total_amount DOUBLE, congestion_surcharge DOUBLE, Airport_fee DOUBLE)" || exit 1
duck -c "INSTALL sqlite; LOAD sqlite; ATTACH 'taxi.sqlite' AS s (TYPE sqlite);
  INSERT INTO s.trips SELECT * FROM read_parquet('$TAXI/*.parquet');" >/dev/null || exit 1
sqlite3 taxi.sqlite "CREATE INDEX trips_pickup ON trips (tpep_pickup_datetime)" || exit 1
say "loaded taxi.sqlite in $(perl -e 'printf "%.1f", $ARGV[1]-$ARGV[0]' "$t0" "$(now)") s"
say "sqlite declared types: $(sqlite3 taxi.sqlite "SELECT group_concat(name || ' ' || type, ', ') FROM pragma_table_info('trips')")"

DUCK_ROOT="sql://duckdb/main.trips?db=$W/taxi.duckdb&range=tpep_pickup_datetime&units=$UNITS"
SQLITE_ROOT="sql://sqlite/main.trips?db=$W/taxi.sqlite&range=tpep_pickup_datetime&units=$UNITS"

# ---- runs --------------------------------------------------------------------
# run NAME ARGS...: one --json call, its exit code and wall time recorded.
run() {
  local name=$1 t0 code
  shift
  say ""
  say "\$ bellwether $*"
  t0=$(now)
  "$BW" "$@" --json >"$OUT/$name.json" 2>"$OUT/$name.err"
  code=$?
  printf '%s %s %s\n' "$name" "$code" "$(perl -e 'printf "%.0f", ($ARGV[1]-$ARGV[0])*1000' "$t0" "$(now)")" >>"$OUT/times.txt"
  say "exit $code, $(tail -1 "$OUT/times.txt" | awk '{print $3}') ms; $(head -c 300 "$OUT/$name.err")"
}
: >"$OUT/times.txt"
LOAD_AT_START=$(uptime | sed 's/.*load averages*: *//')

# 1. Reconcile: the baseline (files against their DuckDB rewrite), then each
#    table against the files.
run reconcile-files reconcile "$TAXI" taxi_duck --key $K --totals fare_amount:dec\(2\) --memory-budget $BUDGET --index "$W/idx-files"
run reconcile-duckdb reconcile "$DUCK_ROOT" "$TAXI" --key $K --totals fare_amount:dec\(2\) --memory-budget $BUDGET --index "$W/ledger-duckdb"
run reconcile-sqlite reconcile "$SQLITE_ROOT" "$TAXI" --key $K --totals fare_amount:dec\(2\) --memory-budget $BUDGET --index "$W/ledger-sqlite"

# 2. Planted differences, in copies of the tables: one changed tip_amount and
#    one deleted row, each on a row whose 4-column key is unique, and one
#    added row with a key the files do not hold.
IFS='|' read -r PV PT PP PF <<<"$(duckdb -no-agent -list -noheader -separator '|' -c "SELECT VendorID, strftime(tpep_pickup_datetime, '%Y-%m-%d %H:%M:%S'), PULocationID, fare_amount
  FROM read_parquet('$TAXI/*.parquet') GROUP BY ALL HAVING count(*) = 1 ORDER BY 2, 1, 3, 4 LIMIT 1 OFFSET 1000")"
IFS='|' read -r DV DT DP DF <<<"$(duckdb -no-agent -list -noheader -separator '|' -c "SELECT VendorID, strftime(tpep_pickup_datetime, '%Y-%m-%d %H:%M:%S'), PULocationID, fare_amount
  FROM read_parquet('$TAXI/*.parquet') GROUP BY ALL HAVING count(*) = 1 ORDER BY 2, 1, 3, 4 LIMIT 1 OFFSET 2000")"
say "planted: tip_amount + 1 on ($PV, $PT, $PP, $PF); deleted ($DV, $DT, $DP, $DF); added a trip by VendorID 9"
MATCH_P="VendorID = $PV AND tpep_pickup_datetime = '$PT' AND PULocationID = $PP AND fare_amount = $PF"
MATCH_D="VendorID = $DV AND tpep_pickup_datetime = '$DT' AND PULocationID = $DP AND fare_amount = $DF"
cp taxi.duckdb planted.duckdb
duck planted.duckdb -c "INSERT INTO trips SELECT * REPLACE (9 AS VendorID) FROM trips WHERE $MATCH_P;
  UPDATE trips SET tip_amount = tip_amount + 1 WHERE $MATCH_P; DELETE FROM trips WHERE $MATCH_D" >/dev/null || exit 1
cp taxi.sqlite planted.sqlite
sqlite3 planted.sqlite "INSERT INTO trips SELECT * FROM trips WHERE $MATCH_P;
  UPDATE trips SET VendorID = 9 WHERE rowid = last_insert_rowid();
  UPDATE trips SET tip_amount = tip_amount + 1 WHERE $MATCH_P; DELETE FROM trips WHERE $MATCH_D" || exit 1
run planted-duckdb reconcile "sql://duckdb/main.trips?db=$W/planted.duckdb&range=tpep_pickup_datetime&units=$UNITS" "$TAXI" --key $K --memory-budget $BUDGET --index "$W/ledger-duckdb"
run planted-sqlite reconcile "sql://sqlite/main.trips?db=$W/planted.sqlite&range=tpep_pickup_datetime&units=$UNITS" "$TAXI" --key $K --memory-budget $BUDGET --index "$W/ledger-sqlite"

# 3. The pickup-zone lookups: over each table (measured), over the files
#    (footer sums), on the indexed files, and inside DuckDB.
run why-duckdb why "$DUCK_ROOT" --from keys/taxi_pu.parquet --index "$W/ledger-duckdb"
run why-sqlite why "$SQLITE_ROOT" --from keys/taxi_pu.parquet --index "$W/ledger-sqlite"
run why-files why "$TAXI" --from keys/taxi_pu.parquet --index "$W/idx-files"
run index-files index "$TAXI" --key exact:PULocationID --index "$W/idx-files"
run locate-files locate "$TAXI" --keys keys/taxi_pu.parquet --index "$W/idx-files"
t0=$(now)
duckdb -no-agent -list -noheader taxi.duckdb -c "EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM trips WHERE PULocationID IN (SELECT PULocationID FROM 'keys/taxi_pu.parquet')" >"$OUT/explain-duckdb.json"
printf 'explain-duckdb 0 %s\n' "$(perl -e 'printf "%.0f", ($ARGV[1]-$ARGV[0])*1000' "$t0" "$(now)")" >>"$OUT/times.txt"
LOAD_AT_END=$(uptime | sed 's/.*load averages*: *//')

# ---- results -----------------------------------------------------------------
MACHINE=$(if [ "$(uname)" = Darwin ]; then
  printf '%s, %s cores, %s GB, macOS %s' "$(sysctl -n machdep.cpu.brand_string)" "$(sysctl -n hw.ncpu)" \
    "$(($(sysctl -n hw.memsize) / 1073741824))" "$(sw_vers -productVersion)"
else
  printf '%s, %s cores, %s' "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')" "$(nproc)" "$(uname -sr)"
fi)
python3 - "$OUT" "$RESULTS" "$MACHINE" "bellwether release build $COMMIT" "$DUCK_VERSION" \
  "load average at start: $LOAD_AT_START; at end: $LOAD_AT_END" <<'PY'
import json, sys, datetime, pathlib
out, results, machine, build, duck_version, notes = sys.argv[1:]
out = pathlib.Path(out)
times = {}
for line in (out / "times.txt").read_text().split("\n"):
    if line.strip():
        name, code, ms = line.split()
        times[name] = (int(code), int(ms))
def load(name):
    return json.loads((out / f"{name}.json").read_text())
date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
rows = []
def row(query, side, metric, value, unit, exactness, build_=build):
    rows.append({"dataset": "taxi", "query": query, "side": side, "metric": metric,
                 "value": value, "unit": unit, "exactness": exactness, "machine": machine,
                 "build": build_, "date": date, "notes": notes})
sides = {"files": "files-vs-files", "duckdb": "duckdb-table", "sqlite": "sqlite-table"}
for k, side in sides.items():
    r = load(f"reconcile-{k}")
    o = r["outcome"]
    row("reconcile", side, "wall_ms", times[f"reconcile-{k}"][1], "ms", "measured")
    row("reconcile", side, "keys_compared", o["counts"]["keys_compared"], "keys", "exact")
    row("reconcile", side, "rows_read", o["memory"]["rows_read"][0], "rows", "exact")
    row("reconcile", side, "passes", o["memory"]["passes"], "passes", "exact")
    row("reconcile", side, "pass", o["pass"], "verdict", "exact")
    row("reconcile", side, "certificate", r["certificate"]["hash"], "hash", "exact")
    for t in r.get("tables", []):
        row("reconcile", side, "units", t["units"], "units", "exact")
        row("reconcile", side, "snapshot", t["snapshot"] or "identities re-read", "discipline", "exact")
for k in ("duckdb", "sqlite"):
    o = load(f"planted-{k}")["outcome"]
    side = sides[k]
    row("reconcile-planted", side, "pass", o["pass"], "verdict", "exact")
    for m in ("mismatched", "missing_left", "missing_right"):
        row("reconcile-planted", side, m, o["counts"][m], "keys", "exact")
    row("reconcile-planted", side, "wall_ms", times[f"planted-{k}"][1], "ms", "measured")
for k, side, exactness in (("duckdb", "duckdb-table", "measured"), ("sqlite", "sqlite-table", "measured"),
                           ("files", "files (footer statistics)", "exact")):
    r = load(f"why-{k}")
    cur = r["current"]
    row("why-pickup-zones", side, "bytes", cur["bytes"], "bytes", exactness)
    row("why-pickup-zones", side, "row_groups", cur["row_groups"], "row groups", "exact")
    row("why-pickup-zones", side, "wall_ms", times[f"why-{k}"][1], "ms", "measured")
    rec = r.get("recommended")
    row("why-pickup-zones", side, "recommended", f"{rec['spec']['kind']}:{','.join(rec['spec']['columns'])}" if rec else "none", "key", "exact")
    row("why-pickup-zones", side, "culprit", (r.get("culprit") or {}).get("reason", {}).get("kind", "none"), "reason", "exact")
loc = load("locate-files")
row("locate-pickup-zones", "files (indexed)", "bytes", loc["planned"]["bytes"], "bytes", "exact")
row("locate-pickup-zones", "files (indexed)", "row_groups", loc["planned"]["row_groups"], "row groups", "exact")
row("locate-pickup-zones", "files (indexed)", "wall_ms", times["locate-files"][1], "ms", "measured")
text = (out / "explain-duckdb.json").read_text()
plan = json.loads(text[text.find("{"):text.rfind("}") + 1])
if plan.get("total_rows_scanned") is not None:
    row("lookup-pickup-zones", "duckdb (engine)", "rows_scanned", plan["total_rows_scanned"], "rows", "measured", duck_version)
row("lookup-pickup-zones", "duckdb (engine)", "wall_ms", times["explain-duckdb"][1], "ms", "measured", duck_version)
ms = {s: next(r["value"] for r in rows if r["query"] == "reconcile" and r["side"] == s and r["metric"] == "wall_ms")
      for s in sides.values()}
keys = next(r["value"] for r in rows if r["query"] == "reconcile" and r["metric"] == "keys_compared")
charts = [{
    "name": "reconcile-wall",
    "metric": "wall_ms",
    "title": "Reconcile 20 million taxi trips: a table against its Parquet files",
    "sides": ["files-vs-files", "duckdb-table", "sqlite-table"],
    "takeaway": (f"Reconciling {keys:,} keys took {ms['duckdb-table']/1000:.0f} s from a DuckDB table and "
                 f"{ms['sqlite-table']/1000:.0f} s from a SQLite table, against {ms['files-vs-files']/1000:.0f} s "
                 "between two Parquet copies; each run read inside one snapshot and wrote a certificate naming both sides."),
}]
doc = {
    "source": "sql",
    "generated_by": "scripts/sql-demo.sh",
    "datasets": {"taxi": {"name": "NYC taxi", "label": "NYC yellow taxi, 2024-01 to 2024-06"}},
    "queries": {
        "reconcile": {"text": "reconcile <side> <taxi Parquet files> --key VendorID,tpep_pickup_datetime,PULocationID,fare_amount --totals fare_amount:dec(2)"},
        "reconcile-planted": {"text": "the same, against a copy of the table with one changed tip_amount, one deleted row, and one added row"},
        "why-pickup-zones": {"text": "why <side> --from keys/taxi_pu.parquet (every pickup zone, as scripts/realdata.sh)"},
        "locate-pickup-zones": {"text": "locate <taxi files> --keys keys/taxi_pu.parquet on index exact:PULocationID"},
        "lookup-pickup-zones": {"text": "EXPLAIN (ANALYZE) SELECT * FROM trips WHERE PULocationID IN (<the key file>), in DuckDB"},
    },
    "charts": charts,
    "rows": rows,
}
pathlib.Path(results).parent.mkdir(parents=True, exist_ok=True)
pathlib.Path(results).write_text(json.dumps(doc, indent=2) + "\n")
print(f"wrote {results}: {len(rows)} rows")
PY
say "results: $RESULTS"
say "transcript: $T; every --json result in $OUT"
