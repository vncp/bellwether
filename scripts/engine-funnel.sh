#!/usr/bin/env bash
# The engine loop of ADR 033 on both bench shapes, against a ClickHouse
# server: the ledger corpus (`bellwether bench generate`) and the market
# corpus (`bellwether bench market`), each loaded into ClickHouse, diagnosed
# on its lake twin, rewritten, and fixed. Prints the funnel each run gives:
# the twin's prediction (exact for the lake), what the engine's log says
# each side of the rewrite read (rows and bytes: the same for every run of
# the same query on the same parts), the certificate, and the receipt.
#
#   CH=http://127.0.0.1:8123/ BW=target/debug/bellwether scripts/engine-funnel.sh OUT
#
# CH is the server's HTTP endpoint (a user with no password that may create
# a database); BW the binary. Writes OUT/<shape>/{why,fix}.json and the
# human output beside them, and drops its database at the end.
set -euo pipefail

OUT=${1:?usage: engine-funnel.sh OUT}
CH=${CH:-http://127.0.0.1:8123/}
BW=${BW:-bellwether}
DB=bw_funnel
HOSTPORT=$(printf '%s' "$CH" | sed -E 's#^https?://([^/]+)/?.*$#\1#')
ENGINE_DB="clickhouse://$HOSTPORT/$DB"

sql() { curl -sS --fail-with-body --data-binary @- "$CH" <<<"$1"; }
load() { # table, parquet files...
  local t=$1; shift
  for f in "$@"; do
    curl -sS --fail-with-body --data-binary @"$f" "${CH}?query=INSERT%20INTO%20$DB.$t%20FORMAT%20Parquet"
  done
}

rm -rf "$OUT" && mkdir -p "$OUT"
sql "DROP DATABASE IF EXISTS $DB"
sql "CREATE DATABASE $DB"
trap 'sql "DROP DATABASE IF EXISTS $DB" >/dev/null 2>&1 || true' EXIT

# One shape: lake folder, table, keys query, before and after queries,
# extra flags for the lake (partitioning).
run() {
  local shape=$1 lake=$2 table=$3 keys=$4 before=$5 after=$6; shift 6
  local dir="$OUT/$shape" engine="$ENGINE_DB/$table"
  mkdir -p "$dir/scratch"
  printf '%s\n' "$keys" >"$dir/keys.sql"
  printf '%s\n' "$before" >"$dir/before.sql"
  printf '%s\n' "$after" >"$dir/after.sql"
  "$BW" "$@" --json workload record "$lake" --from-engine "$engine" --keys-query "$dir/keys.sql" \
    --scratch "$dir/scratch" >"$dir/workload.json"
  local wl; wl=$(jq -r .workload "$dir/workload.json")
  "$BW" "$@" why "$lake" --workload "$wl" --twin-of "$engine" --gate in:10 --gate join:100000 \
    >"$dir/why.txt"
  "$BW" "$@" --json why "$lake" --workload "$wl" --twin-of "$engine" --gate in:10 --gate join:100000 \
    >"$dir/why.json"
  local ev; ev=$(jq -r .evidence.hash "$dir/why.json")
  "$BW" "$@" --json propose "$lake" --rewrite "$dir/before.sql=$dir/after.sql" --evidence "$ev" \
    --by engine-funnel >"$dir/propose.json"
  local p; p=$(jq -r .proposal.hash "$dir/propose.json")
  "$BW" "$@" fix "$lake" --twin-of "$engine" --proposal "$p" --scratch "$dir/scratch" \
    | tee "$dir/fix.txt"
  local r; r=$(grep -Eo 'receipt [0-9a-f]{64}' "$dir/fix.txt" | head -1 | cut -d' ' -f2)
  "$BW" "$@" receipt show "$lake" "$r" >"$dir/receipt.txt"
  "$BW" "$@" --json receipt show "$lake" "$r" >"$dir/receipt.json"
  echo "== $shape: twin"
  jq -r '.twin.of | "engine rows \(.engine_rows) in \(.units) units; lake rows \(.lake_rows) at sequence \(.sequence); \(.keys) keys; predicted \(.predicted_rows) rows, \(.predicted_row_groups) row groups, \(.predicted_files) files"' "$dir/why.json"
  jq -r '.twin.lints[]? | "lint \(.rule) [\(.columns | join(", "))]"' "$dir/why.json"
  echo "== $shape: receipt"
  cat "$dir/receipt.txt"
}

# --- the ledger shape --------------------------------------------------
"$BW" bench generate --out "$OUT/ledger-corpus" --seed 7 --days 8 --files 32 \
  --row-groups-per-file 4 --rows-per-row-group 2000 --entities 20000 --churn 0.1 >/dev/null
sql "CREATE TABLE $DB.ledger (entity_id UInt64, batch_id Int64, version_ts DateTime64(6, 'UTC'),
  status String, amount Decimal(18, 2), payload String) ENGINE = MergeTree
  PARTITION BY toYYYYMMDD(version_ts) ORDER BY (batch_id, entity_id)"
load ledger "$OUT"/ledger-corpus/data/*.parquet
run ledger "$OUT/ledger-corpus/data" ledger \
  "SELECT DISTINCT entity_id FROM $DB.ledger WHERE entity_id % 1000 = 7" \
  "SELECT * FROM (SELECT entity_id, version_ts, amount, status FROM $DB.ledger ORDER BY version_ts DESC LIMIT 1 BY entity_id) WHERE entity_id % 1000 = 7" \
  "SELECT entity_id, version_ts, amount, status FROM $DB.ledger WHERE entity_id % 1000 = 7 ORDER BY version_ts DESC LIMIT 1 BY entity_id"

# --- the time-series shape ---------------------------------------------
"$BW" bench market --out "$OUT/market-corpus" --rung small >/dev/null
sql "CREATE TABLE $DB.trades (symbol String, ts DateTime64(6, 'UTC'), known_at DateTime64(6, 'UTC'),
  price Float64, size Int64) ENGINE = MergeTree PARTITION BY toYYYYMMDD(ts) ORDER BY ts"
load trades "$OUT"/market-corpus/data/day=*/*.parquet
run market "$OUT/market-corpus/data" trades \
  "SELECT DISTINCT symbol FROM $DB.trades WHERE symbol IN ('S0003', 'S0017', 'S0029')" \
  "SELECT * FROM (SELECT symbol, ts, known_at, price, size FROM $DB.trades ORDER BY ts DESC, known_at DESC LIMIT 1 BY symbol) WHERE symbol IN ('S0003', 'S0017', 'S0029')" \
  "SELECT symbol, ts, known_at, price, size FROM $DB.trades WHERE symbol IN ('S0003', 'S0017', 'S0029') ORDER BY ts DESC, known_at DESC LIMIT 1 BY symbol" \
  --partitioning hive
