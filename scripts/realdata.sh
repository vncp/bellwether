#!/usr/bin/env bash
# Reproduces docs/measured/2026-10-24-realdata.md: every CLI verb over public
# data nobody here generated (NYC taxi trips, GH Archive events).
#
#   REALDATA_DIR=/some/scratch scripts/realdata.sh            # everything
#   BELLWETHER=/path/to/bellwether scripts/realdata.sh        # skip the build
#
# Needs: curl, duckdb (2.x, run as `duckdb -no-agent`), jq, perl, cargo.
# Runs on macOS and Linux.
# Downloads about 1.2 GB (6 taxi months, 12 GH Archive hours) into
# $REALDATA_DIR/dl and writes everything else under $REALDATA_DIR. Deletes
# nothing; remove $REALDATA_DIR (and the release target dir) when done.
#
# Every bellwether call is printed as `$ bellwether ...`, followed by its
# output (stdout and stderr), its exit code, and its wall time. Read-only
# verbs (and `why`, `index`, `reconcile`, `receipt`, `report`) run twice:
# once for the human output, then again with --json into $OUT/json. The
# second call is a real second call: `why` writes its own records (new
# hashes), `index` finds nothing to do. Hashes used downstream are always
# taken from the --json run. `propose` and `fix` change state, so they run
# once, with --json.

set -uo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
R=${REALDATA_DIR:-${TMPDIR:-/tmp}/bellwether-realdata}
OUT=$R/out
W=$R/work
DL=$R/dl
TAXI_MONTHS="01 02 03 04 05 06"
GH_HOURS=$(seq 0 11) # 12 of 24 hours: keeps total downloads under 2 GB
mkdir -p "$OUT/human" "$OUT/json" "$W" "$DL/taxi" "$DL/gh"
T=$OUT/transcript.txt
: >"$T"

say() { printf '%s\n' "$*" | tee -a "$T"; }
now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
since() { perl -e 'printf "%.3f", $ARGV[1] - $ARGV[0]' "$1" "$(now)"; }
duck() { duckdb -no-agent -csv -noheader -c "$1"; }

# ---- preflight ---------------------------------------------------------------
free_gb=$(df -Pk / | awk 'NR==2 {print int($4 / 1048576)}')
if [ "$free_gb" -lt 8 ]; then
  echo "only ${free_gb} GB free on /; stopping (need 8)" >&2
  exit 1
fi

if [ -z "${BELLWETHER:-}" ]; then
  export CARGO_INCREMENTAL=0
  export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/bellwether-target-realdata}
  (cd "$REPO" && cargo build --release -p bellwether-cli) || exit 1
  BELLWETHER=$CARGO_TARGET_DIR/release/bellwether
fi
BW=$BELLWETHER

# ---- downloads (verified against Content-Length) ----------------------------
# shellcheck source=datasets.sh
. "$REPO/scripts/datasets.sh"
bw_fetch_taxi "$DL" "$TAXI_MONTHS"
bw_fetch_gh "$DL" "$GH_HOURS"

# ---- datasets ----------------------------------------------------------------
cd "$W" || exit 1
rm -rf taxi taxi_duck taxi_duck_mut taxi_dec taxi_dec_duck gh ghfix1000 ghfix10 ghjson ghnested keys idx-counts
mkdir -p taxi taxi_duck taxi_duck_mut taxi_dec taxi_dec_duck gh ghjson ghnested keys
cp "$DL"/taxi/*.parquet taxi/

bw_flatten_gh "$DL" gh "$GH_HOURS"
cp -r gh ghfix1000
cp -r gh ghfix10
# Probes of the raw nested data: hour 0 as JSON lines (named .json, then
# .jsonl), and hour 0 as Parquet with its struct columns kept.
gunzip -c "$DL/gh/2024-01-01-0.json.gz" >ghjson/2024-01-01-0.json
duckdb -no-agent -c "SET TimeZone='UTC'; COPY (SELECT id, type, actor, repo, created_at
  FROM read_json('$DL/gh/2024-01-01-0.json.gz', format='newline_delimited', maximum_object_size=100000000))
  TO 'ghnested/2024-01-01-00.parquet' (FORMAT PARQUET)" >/dev/null

# The taxi folder rewritten by a different writer (DuckDB), file for file.
for m in $TAXI_MONTHS; do
  f=yellow_tripdata_2024-$m.parquet
  duckdb -no-agent -c "COPY (SELECT * FROM read_parquet('taxi/$f')) TO 'taxi_duck/$f' (FORMAT PARQUET)" >/dev/null
done

# Key files: N distinct values, chosen by a seeded hash so the choice does
# not depend on DuckDB's thread scheduling (a reservoir sample over a
# parallel DISTINCT picked different values on different runs).
sample() { # sample GLOB COL N OUT
  duckdb -no-agent -c "COPY (SELECT $2 FROM (SELECT DISTINCT $2 FROM read_parquet('$1') WHERE $2 IS NOT NULL)
    ORDER BY hash($2, 42), $2 LIMIT $3) TO '$4' (FORMAT PARQUET)" >/dev/null
}
sample 'taxi/*.parquet' PULocationID 1000 keys/taxi_pu.parquet
sample 'taxi/*.parquet' VendorID 1000 keys/taxi_vendor.parquet
sample 'gh/*.parquet' repo_id 1000 keys/gh_repo.parquet
sample 'gh/*.parquet' actor_id 1000 keys/gh_actor.parquet
sample 'gh/*.parquet' repo_id 10 keys/gh_repo10.parquet

# ---- transcript helpers -----------------------------------------------------
run() { # run NAME ARGS...: human output, then the same call with --json
  local name=$1 t0 code
  shift
  say ""
  say "\$ bellwether $*"
  t0=$(now)
  "$BW" "$@" >"$OUT/human/$name.txt" 2>&1
  code=$?
  local dt
  dt=$(since "$t0")
  tee -a "$T" <"$OUT/human/$name.txt"
  say "exit $code  ($dt s)"
  t0=$(now)
  "$BW" "$@" --json >"$OUT/json/$name.json" 2>"$OUT/json/$name.err"
  code=$?
  say "  (--json: exit $code, $(since "$t0") s, json/$name.json)"
}
once() { # once NAME ARGS...: one call, --json (state-changing verbs)
  local name=$1 t0 code
  shift
  say ""
  say "\$ bellwether $* --json"
  t0=$(now)
  "$BW" "$@" --json >"$OUT/json/$name.json" 2>"$OUT/json/$name.err"
  code=$?
  local dt
  dt=$(since "$t0")
  # Embedded ledger records are elided for the transcript; the full line is
  # in json/NAME.json.
  jq -c 'walk(if type == "object" and has("record") then .record = "..." else . end)' \
    "$OUT/json/$name.json" 2>/dev/null | tee -a "$T"
  tee -a "$T" <"$OUT/json/$name.err"
  say "exit $code  ($dt s)"
}
j() { jq -r "$2" "$OUT/json/$1.json"; }
check() { # check LABEL A B
  if [ "$2" = "$3" ]; then say "CHECK PASS  $1: $2 = $3"; else say "CHECK FAIL  $1: $2 != $3"; fi
}
# The plan's (file, row group) pairs from a lookup's Evidence, as name:rg.
plan_pairs() {
  jq -r '.evidence.record.body.plan.entries[] | (.file.path | split("/") | last) as $f | .row_groups[] | "\($f):\(.)"' \
    "$OUT/json/$1.json" | sort
}
# Oracle from the rows themselves (DuckDB): every row group holding a key
# (MODE=locate), a key's newest row (MODE=latest), or a key's newest row at
# or before the instant AT, a SQL literal (MODE=asof).
oracle_pairs() { # oracle_pairs DIR KEYCOL KEYS MODE [TSCOL [AT]]
  local filter
  if [ "$4" = latest ]; then
    filter="(r.$2, r.$5) IN (SELECT $2, max($5) FROM read_parquet('$1/*.parquet') WHERE $2 IN (SELECT $2 FROM read_parquet('$3')) GROUP BY 1)"
  elif [ "$4" = asof ]; then
    filter="(r.$2, r.$5) IN (SELECT $2, max($5) FROM read_parquet('$1/*.parquet') WHERE $2 IN (SELECT $2 FROM read_parquet('$3')) AND $5 <= $6 GROUP BY 1)"
  else
    filter="r.$2 IN (SELECT $2 FROM read_parquet('$3'))"
  fi
  duck "WITH rg AS (SELECT DISTINCT file_name, row_group_id, row_group_num_rows AS n FROM parquet_metadata('$1/*.parquet')),
    rgc AS (SELECT *, SUM(n) OVER (PARTITION BY file_name ORDER BY row_group_id) - n AS lo FROM rg)
    SELECT DISTINCT regexp_extract(r.filename, '[^/]+\$') || ':' || rgc.row_group_id
    FROM read_parquet('$1/*.parquet', filename=true, file_row_number=true) r
    JOIN rgc ON rgc.file_name = r.filename AND r.file_row_number >= rgc.lo AND r.file_row_number < rgc.lo + rgc.n
    WHERE $filter" | sort
}
# Compressed bytes of a set of name:rg pairs, from the footers (DuckDB).
pair_bytes() { # pair_bytes DIR PAIRSFILE
  duck "SELECT sum(total_compressed_size) FROM parquet_metadata('$1/*.parquet')
    WHERE regexp_extract(file_name, '[^/]+\$') || ':' || row_group_id
      IN (SELECT column0 FROM read_csv('$2', header=false, columns={'column0': 'VARCHAR'}))"
}

say "# bellwether on real public data"
say "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
if [ "$(uname -s)" = Darwin ]; then
  machine="$(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.ncpu) cores, $(($(sysctl -n hw.memsize) / 1073741824)) GB, macOS $(sw_vers -productVersion)"
else
  machine="$(uname -m), $(nproc) cores, $(awk '/^MemTotal:/ {print int($2 / 1048576)}' /proc/meminfo) GB, $(uname -sr)"
fi
if [ -n "${CI:-}" ]; then disk="CI runner disk"; else disk="local SSD"; fi
say "machine: $machine, $disk"
say "build: release, $(rustc -V 2>/dev/null), commit $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null)"
say "tools: $(duckdb -no-agent --version 2>/dev/null | head -1)"
say "taxi: $(duck "SELECT count(*) || ' rows; ' || count(DISTINCT PULocationID) || ' distinct PULocationID; ' || count(DISTINCT VendorID) || ' distinct VendorID' FROM read_parquet('taxi/*.parquet')")"
say "gh: $(duck "SET TimeZone='UTC'; SELECT count(*) || ' rows; ' || count(DISTINCT repo_id) || ' distinct repo_id; ' || count(DISTINCT actor_id) || ' distinct actor_id; ' || min(created_at) || ' .. ' || max(created_at) FROM read_parquet('gh/*.parquet')")"
say "key files: taxi_pu $(duck "SELECT count(*) FROM 'keys/taxi_pu.parquet'") values (all of them), taxi_vendor $(duck "SELECT count(*) FROM 'keys/taxi_vendor.parquet'"), gh_repo $(duck "SELECT count(*) FROM 'keys/gh_repo.parquet'"), gh_actor $(duck "SELECT count(*) FROM 'keys/gh_actor.parquet'"), gh_repo10 $(duck "SELECT count(*) FROM 'keys/gh_repo10.parquet'")"
say "4-column reconcile key duplicates (rows minus distinct keys): $(duck "SELECT count(*) - count(DISTINCT (VendorID, tpep_pickup_datetime, PULocationID, fare_amount)) FROM read_parquet('taxi/*.parquet')")"
say "working directory: $W"

# ---- 0. what the text source accepts ----------------------------------------
say ""
say "## 0. GH Archive as raw JSON lines (nested), before flattening"
run 00-json-dotjson why ghjson --format jsonl
mv ghjson/2024-01-01-0.json ghjson/2024-01-01-0.jsonl
run 00-json-why why ghjson --format jsonl
run 00-json-cols why ghjson --format jsonl --columns repo.id
once 00-json-index index ghjson --format jsonl --key exact:id
run 00-nested-parquet-why why ghnested
once 00-nested-parquet-index index ghnested --key exact:id
rm -f ghjson/2024-01-01-0.jsonl # 500 MB

# ---- 1. why ------------------------------------------------------------------
say ""
say "## 1. why"
run 10-taxi-why why taxi
run 11-taxi-why-pu why taxi --from keys/taxi_pu.parquet
run 12-taxi-why-vendor why taxi --from keys/taxi_vendor.parquet
run 13-gh-why why gh
run 14-gh-why-repo why gh --from keys/gh_repo.parquet
run 15-gh-why-actor why gh --from keys/gh_actor.parquet
run 16-gh-why-repo10 why gh --from keys/gh_repo10.parquet
for n in 10-taxi-why 11-taxi-why-pu 12-taxi-why-vendor 13-gh-why 14-gh-why-repo 15-gh-why-actor 16-gh-why-repo10; do
  say "summary $n: recorded=$(j $n '.evidence.recorded') lints=$(j $n '[.profile.lints[] | .rule // .id // .kind] | join(",")') culprit=$(j $n '.culprit.columns // .culprit | tostring') recommended=$(j $n '.recommended.spec // "none" | tostring')"
done

# ---- 2. index, locate, latest, asof ----------------------------------------
say ""
say "## 2. index and lookups (recommended key where there is one, else the floor, labeled)"
run 20-taxi-index index taxi --key exact:PULocationID --key temporal:PULocationID@tpep_pickup_datetime
run 21-taxi-locate locate taxi --keys keys/taxi_pu.parquet
run 22-taxi-latest latest taxi --keys keys/taxi_pu.parquet
run 23-taxi-asof asof taxi --keys keys/taxi_pu.parquet --at 2024-03-01T00:00:00Z
check "taxi why floor bytes = locate planned bytes" \
  "$(j 11-taxi-why-pu '.candidates[] | select(.spec.columns == ["PULocationID"] and .spec.kind == "exact") | .proposed.bytes')" \
  "$(j 21-taxi-locate '.planned.bytes')"

run 24-gh-index index gh --key exact:repo_id --key temporal:repo_id@created_at
run 25-gh-locate locate gh --keys keys/gh_repo.parquet
run 26-gh-latest latest gh --keys keys/gh_repo.parquet
run 27-gh-asof asof gh --keys keys/gh_repo.parquet --at 2024-01-01T06:00:00Z
run 28-gh-locate10 locate gh --keys keys/gh_repo10.parquet
run 29-gh-latest10 latest gh --keys keys/gh_repo10.parquet
run 2a-gh-asof10 asof gh --keys keys/gh_repo10.parquet --at 2024-01-01T06:00:00Z
check "gh 1,000 keys: why floor bytes = locate planned bytes" \
  "$(j 14-gh-why-repo '.candidates[] | select(.spec.columns == ["repo_id"] and .spec.kind == "exact") | .proposed.bytes')" \
  "$(j 25-gh-locate '.planned.bytes')"
check "gh 10 keys: why recommended bytes = locate planned bytes" \
  "$(j 16-gh-why-repo10 '.recommended.proposed.bytes')" "$(j 28-gh-locate10 '.planned.bytes')"
for k in gh_repo:25-gh-locate gh_repo10:28-gh-locate10; do
  kf=${k%%:*}
  n=${k#*:}
  plan_pairs "$n" >"$OUT/$n.plan"
  oracle_pairs gh repo_id "keys/$kf.parquet" locate >"$OUT/$n.oracle"
  check "$n: plan row groups = DuckDB oracle row groups (exact index)" \
    "$(wc -l <"$OUT/$n.plan" | tr -d ' ') $(shasum "$OUT/$n.plan" | cut -c1-12)" \
    "$(wc -l <"$OUT/$n.oracle" | tr -d ' ') $(shasum "$OUT/$n.oracle" | cut -c1-12)"
  check "$n: planned bytes = footer compressed bytes of the oracle's row groups" \
    "$(j "$n" '.planned.bytes')" "$(pair_bytes gh "$OUT/$n.oracle")"
done
# latest and asof: every row group holding a wanted version must be planned
# (the plan may hold more; the residual picks the version).
temporal_check() { # temporal_check NAME DIR KEYCOL KEYS MODE TSCOL [AT]
  local n=$1
  plan_pairs "$n" >"$OUT/$n.plan"
  oracle_pairs "$2" "$3" "$4" "$5" "$6" "${7:-}" >"$OUT/$n.oracle"
  local missing
  missing=$(comm -13 "$OUT/$n.plan" "$OUT/$n.oracle" | wc -l | tr -d ' ')
  say "$n: plan $(wc -l <"$OUT/$n.plan" | tr -d ' ') row groups; row groups holding a wanted version (DuckDB) $(wc -l <"$OUT/$n.oracle" | tr -d ' ')"
  check "$n: wanted-version row groups missing from the plan" "$missing" 0
}
temporal_check 22-taxi-latest taxi PULocationID keys/taxi_pu.parquet latest tpep_pickup_datetime
temporal_check 23-taxi-asof taxi PULocationID keys/taxi_pu.parquet asof tpep_pickup_datetime "TIMESTAMP '2024-03-01 00:00:00'"
temporal_check 26-gh-latest gh repo_id keys/gh_repo.parquet latest created_at
temporal_check 27-gh-asof gh repo_id keys/gh_repo.parquet asof created_at "TIMESTAMPTZ '2024-01-01 06:00:00+00'"
temporal_check 29-gh-latest10 gh repo_id keys/gh_repo10.parquet latest created_at
temporal_check 2a-gh-asof10 gh repo_id keys/gh_repo10.parquet asof created_at "TIMESTAMPTZ '2024-01-01 06:00:00+00'"

# ---- 3. reconcile -----------------------------------------------------------
say ""
say "## 3. reconcile (no unique key exists: multiset mode, no --latest-by)"
K=VendorID,tpep_pickup_datetime,PULocationID,fare_amount
run 30-reconcile-totals reconcile taxi taxi_duck --key $K --totals fare_amount
run 31-reconcile reconcile taxi taxi_duck --key $K
# Mutate one row of the copy: tip_amount (not a key column) on a row whose
# 4-column key is unique, in the March file.
MUT=$(duck "SELECT VendorID || '|' || strftime(tpep_pickup_datetime, '%Y-%m-%d %H:%M:%S') || '|' || PULocationID || '|' || fare_amount
  FROM read_parquet('taxi_duck/yellow_tripdata_2024-03.parquet') GROUP BY VendorID, tpep_pickup_datetime, PULocationID, fare_amount
  HAVING count(*) = 1 ORDER BY tpep_pickup_datetime, VendorID, PULocationID, fare_amount LIMIT 1 OFFSET 1000")
IFS='|' read -r MV MT MP MF <<<"$MUT"
say "mutation: tip_amount + 1.00 on the row VendorID=$MV tpep_pickup_datetime=$MT PULocationID=$MP fare_amount=$MF (taxi_duck_mut, March)"
cp taxi_duck/*.parquet taxi_duck_mut/
duckdb -no-agent -c "COPY (SELECT * REPLACE (CASE WHEN VendorID = $MV AND tpep_pickup_datetime = TIMESTAMP '$MT'
  AND PULocationID = $MP AND fare_amount = $MF THEN tip_amount + 1.0 ELSE tip_amount END AS tip_amount)
  FROM read_parquet('taxi_duck/yellow_tripdata_2024-03.parquet'))
  TO 'taxi_duck_mut/yellow_tripdata_2024-03.parquet' (FORMAT PARQUET)" >/dev/null
run 32-reconcile-mut reconcile taxi taxi_duck_mut --key $K
say "from json/32-reconcile-mut.json: mismatches $(j 32-reconcile-mut '[.outcome.mismatches[] | {kind, key: .key.values}] | tostring')"
for n in 31-reconcile 32-reconcile-mut; do
  say "from json/$n.json: memory $(j $n '.outcome.memory | {budget_bytes, passes, peak_state_bytes, rows_read} | tostring')"
done
# WORKAROUND (labeled): --totals takes decimal columns only, and fare_amount
# is a double in the source. January only, both sides cast to DECIMAL(10,2) by
# DuckDB, one from the original file and one from the DuckDB copy.
duckdb -no-agent -c "COPY (SELECT * REPLACE (fare_amount::DECIMAL(10,2) AS fare_amount) FROM read_parquet('taxi/yellow_tripdata_2024-01.parquet')) TO 'taxi_dec/jan.parquet' (FORMAT PARQUET)" >/dev/null
duckdb -no-agent -c "COPY (SELECT * REPLACE (fare_amount::DECIMAL(10,2) AS fare_amount) FROM read_parquet('taxi_duck/yellow_tripdata_2024-01.parquet')) TO 'taxi_dec_duck/jan.parquet' (FORMAT PARQUET)" >/dev/null
say "WORKAROUND: decimal copies of January for --totals (fare_amount cast to DECIMAL(10,2) on both sides)"
run 33-reconcile-dec-totals reconcile taxi_dec taxi_dec_duck --key $K --totals fare_amount --group-by VendorID
say "DuckDB fare_amount total, January, by VendorID: $(duck "SELECT string_agg(VendorID || '=' || s, ' ' ORDER BY VendorID) FROM (SELECT VendorID, sum(fare_amount) s FROM 'taxi_dec/jan.parquet' GROUP BY 1)")"

# ---- 3b. count ---------------------------------------------------------------
# count(*) where key in (...) from per-key counts (exact:<key>,counts), one
# index location per folder under idx-counts/, so the indexes above are
# unchanged. Keys: an integer (PULocationID, repo_id), a timestamp
# (created_at) and a decimal (fare_amount in the decimal January copy). The
# oracle is DuckDB over the rows: count(*) of the rows whose key is in the
# key file, and the row groups holding one (oracle_pairs).
say ""
say "## 3b. count from per-key counts, against DuckDB"
sample 'gh/*.parquet' created_at 1000 keys/gh_created.parquet
sample 'taxi_dec/*.parquet' fare_amount 1000 keys/taxi_fare.parquet
count_check() { # count_check NAME DIR KEYCOL KEYS
  check "$1: count rows = DuckDB count(*)" "$(j "$1" '.rows')" \
    "$(duck "SELECT count(*) FROM read_parquet('$2/*.parquet') WHERE $3 IN (SELECT $3 FROM read_parquet('$4'))")"
  check "$1: count row groups = DuckDB row groups holding a key" "$(j "$1" '.row_groups')" \
    "$(oracle_pairs "$2" "$3" "$4" locate | wc -l | tr -d ' ')"
}
once 34-taxi-count-index index taxi --index idx-counts/taxi --key exact:PULocationID,counts
run 35-taxi-count count taxi --index idx-counts/taxi --keys keys/taxi_pu.parquet
count_check 35-taxi-count taxi PULocationID keys/taxi_pu.parquet
once 36-gh-count-index index gh --index idx-counts/gh --key exact:repo_id,counts --key exact:created_at,counts
run 37-gh-count count gh --index idx-counts/gh --keys keys/gh_repo.parquet
count_check 37-gh-count gh repo_id keys/gh_repo.parquet
run 38-gh-count10 count gh --index idx-counts/gh --keys keys/gh_repo10.parquet
count_check 38-gh-count10 gh repo_id keys/gh_repo10.parquet
run 39-gh-count-ts count gh --index idx-counts/gh --keys keys/gh_created.parquet
count_check 39-gh-count-ts gh created_at keys/gh_created.parquet
once 3a-dec-count-index index taxi_dec --index idx-counts/taxi_dec --key exact:fare_amount,counts
run 3b-dec-count count taxi_dec --index idx-counts/taxi_dec --keys keys/taxi_fare.parquet
count_check 3b-dec-count taxi_dec fare_amount keys/taxi_fare.parquet

# ---- 4. fix ------------------------------------------------------------------
say ""
say "## 4. propose, fix, receipt, report"
say "### 4a. ghfix1000: the 1,000-key workload (why recommends nothing)"
run 40-fix1000-why why ghfix1000 --from keys/gh_repo.parquet
EV=$(j 40-fix1000-why '.evidence.hash')
WL=$(j 40-fix1000-why '.workload.hash')
once 41-fix1000-propose propose ghfix1000 --evidence "$EV" --by realdata --key exact:repo_id
P=$(j 41-fix1000-propose '.hash // .proposal.hash // .written.hash')
once 42-fix1000-fix fix ghfix1000 --proposal "$P" --workload "$WL"
say "from json/42-fix1000-fix.json: outcome $(j 42-fix1000-fix '.outcome.kind'), score current $(j 42-fix1000-fix '.score.current.bytes') bytes, proposed $(j 42-fix1000-fix '.score.proposed.bytes') bytes"
R1=$(j 42-fix1000-fix '.. | objects | select(has("receipt")) | .receipt | if type == "object" then .hash else . end' | head -1)
[ -n "$R1" ] && [ "$R1" != null ] && run 43-fix1000-receipt receipt show ghfix1000 "$R1"

say ""
say "### 4b. ghfix10: the 10-key workload (why recommends exact:repo_id)"
run 44-fix10-why why ghfix10 --from keys/gh_repo10.parquet
EV=$(j 44-fix10-why '.evidence.hash')
WL=$(j 44-fix10-why '.workload.hash')
once 45-fix10-propose propose ghfix10 --evidence "$EV" --by realdata --key exact:repo_id
P=$(j 45-fix10-propose '.hash // .proposal.hash // .written.hash')
run 46-fix10-verify fix ghfix10 --proposal "$P" --workload "$WL" --verify-only
once 47-fix10-fix fix ghfix10 --proposal "$P" --workload "$WL"
R2=$(j 47-fix10-fix '.. | objects | select(has("receipt")) | .receipt | if type == "object" then .hash else . end' | head -1)
run 48-fix10-receipt receipt show ghfix10 "$R2"
mkdir -p "$OUT/report"
run 49-fix10-report report ghfix10 --html "$OUT/report/ghfix10.html"
run 4a-fix10-locate locate ghfix10 --keys keys/gh_repo10.parquet
check "ghfix10: receipt's predicted bytes = locate planned bytes after apply" \
  "$(j 44-fix10-why '.recommended.proposed.bytes')" "$(j 4a-fix10-locate '.planned.bytes')"
say "report: $(wc -c <"$OUT/report/ghfix10.html" | tr -d ' ') bytes; script tags: $(grep -c '<script' "$OUT/report/ghfix10.html"); external refs: $(grep -Eoc '(src|href)="https?:' "$OUT/report/ghfix10.html")"

say ""
say "done: transcript $T, json $OUT/json"
