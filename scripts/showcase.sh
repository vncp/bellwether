#!/usr/bin/env bash
# Reproduces the README's "Results" section and docs/measured/showcase.md:
# realistic questions on four datasets, each answered three ways (a full scan,
# DuckDB on its own, Bellwether's plan after `why` -> `fix`), plus `reconcile`
# against a DuckDB EXCEPT ALL, then the charts from the results file.
#
#   scripts/showcase.sh                                  # everything
#   BELLWETHER=/path/to/bellwether BELLWETHER_BUILD="release build of <sha> (rustc x)" \
#     scripts/showcase.sh                                # skip the build
#   SHOWCASE_DIR=/some/scratch scripts/showcase.sh       # where data and indexes go
#   scripts/showcase.sh --only gh-repos10,rec-gh-planted # re-measure some ids
#   scripts/showcase.sh --market-hive                    # market with hive partitioning
#   scripts/showcase.sh --accept-new-inputs              # after a DuckDB upgrade or a
#                                                        # republished download
#
# --market-hive needs an engine whose `fix` records --partitioning in the
# index it builds. Without it the market folder is read as plain files (the
# questions never filter on `day`, so no byte changes).
#
# Needs: curl, duckdb (2.x, run as `duckdb -no-agent`), python3, cargo.
# Downloads about 2.6 GB (6 taxi months, 12 GH Archive hours, 10 ClickBench
# files) into $SHOWCASE_DIR/dl, verified against Content-Length, and writes
# everything else under $SHOWCASE_DIR. Deletes nothing outside it; remove
# $SHOWCASE_DIR (and the release target dir) when done. Writes
# docs/measured/results/showcase.json and re-renders the charts, the README
# table, and the measured page from it (scripts/render_charts.py).

set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
R=${SHOWCASE_DIR:-${TMPDIR:-/tmp}/bellwether-showcase}
DL=$R/dl
W=$R/work
TAXI_MONTHS="01 02 03 04 05 06"
GH_HOURS=$(seq 0 11)
CB_PARTS=$(seq 0 9)
mkdir -p "$DL" "$W"

free_gb=$(df -Pk / | awk 'NR==2 {print int($4 / 1048576)}')
if [ "$free_gb" -lt 8 ]; then
  echo "only ${free_gb} GB free on /; stopping (need 8)" >&2
  exit 1
fi

if [ -z "${BELLWETHER:-}" ]; then
  export CARGO_INCREMENTAL=0
  export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/bellwether-target-showcase}
  (cd "$REPO" && cargo build --release -p bellwether-cli)
  BELLWETHER=$CARGO_TARGET_DIR/release/bellwether
  # The engine's sources, not this branch's docs: the commit of the newest
  # change to crates/ or Cargo.lock.
  ENGINE=$(git -C "$REPO" log -1 --format=%h -- crates Cargo.lock Cargo.toml)
  RUSTC=$(PATH="$HOME/.cargo/bin:$PATH" rustc -V 2>/dev/null | cut -d' ' -f1-2 || true)
  BUILD="bellwether release build of $ENGINE (${RUSTC:-rustc unknown})"
elif [ -n "${BELLWETHER_BUILD:-}" ]; then
  BUILD=$BELLWETHER_BUILD
else
  echo "BELLWETHER is set, so this script did not build it: set BELLWETHER_BUILD to how it was built" >&2
  echo "(for example \"bellwether release build of eca48d1 (rustc 1.99.1)\"); every result row records it" >&2
  exit 2
fi
BW=$BELLWETHER
if [ -n "${GITHUB_ACTIONS:-}" ]; then DISK="CI runner disk"; else DISK=${SHOWCASE_DISK:-local SSD}; fi

# ---- downloads ---------------------------------------------------------------
# shellcheck source=datasets.sh
. "$REPO/scripts/datasets.sh"
bw_fetch_taxi "$DL" "$TAXI_MONTHS"
bw_fetch_gh "$DL" "$GH_HOURS"
bw_fetch_clickbench "$DL" "$CB_PARTS"
# The bytes every number rests on; showcase.py records them and refuses a
# rerun on different files (a republished month, say) unless told to accept it.
(cd "$DL" && if command -v sha256sum >/dev/null; then sha256sum taxi/*.parquet gh/*.json.gz clickbench/*.parquet;
  else shasum -a 256 taxi/*.parquet gh/*.json.gz clickbench/*.parquet; fi) >"$W/downloads.sha256"

# ---- datasets (rebuilt every run; indexes live under work/idx) --------------
cd "$W"
rm -rf taxi taxi_duck taxi_duck_mut gh gh_copy gh_mut clickbench market idx keys
mkdir -p taxi taxi_duck taxi_duck_mut gh_copy gh_mut clickbench idx keys
# Hard links where the filesystem allows: nothing writes to the data files.
link() { ln "$1" "$2" 2>/dev/null || cp "$1" "$2"; }
for f in "$DL"/taxi/*.parquet; do link "$f" taxi/; done
for f in "$DL"/clickbench/*.parquet; do link "$f" clickbench/; done
bw_flatten_gh "$DL" gh "$GH_HOURS"

# Reconcile inputs. The taxi folder rewritten file for file by another writer
# (DuckDB), and that rewrite with one tip changed on a March trip whose
# 4-column key is unique. GH: a copy, and a copy with one event's type changed
# on a row whose (repo_id, actor_id, created_at) is unique.
for f in taxi/*.parquet; do
  duckdb -no-agent -c "COPY (SELECT * FROM read_parquet('$f')) TO 'taxi_duck/$(basename "$f")' (FORMAT PARQUET)" >/dev/null
done
for f in taxi_duck/*.parquet; do link "$f" taxi_duck_mut/; done
MUT=$(duckdb -no-agent -csv -noheader -c "SELECT VendorID || '|' || strftime(tpep_pickup_datetime, '%Y-%m-%d %H:%M:%S') || '|' || PULocationID || '|' || fare_amount
  FROM read_parquet('taxi_duck/yellow_tripdata_2024-03.parquet') GROUP BY VendorID, tpep_pickup_datetime, PULocationID, fare_amount
  HAVING count(*) = 1 ORDER BY tpep_pickup_datetime, VendorID, PULocationID, fare_amount LIMIT 1 OFFSET 1000")
IFS='|' read -r MV MT MP MF <<<"$MUT"
rm taxi_duck_mut/yellow_tripdata_2024-03.parquet
duckdb -no-agent -c "COPY (SELECT * REPLACE (CASE WHEN VendorID = $MV AND tpep_pickup_datetime = TIMESTAMP '$MT'
  AND PULocationID = $MP AND fare_amount = $MF THEN tip_amount + 1.0 ELSE tip_amount END AS tip_amount)
  FROM read_parquet('taxi_duck/yellow_tripdata_2024-03.parquet'))
  TO 'taxi_duck_mut/yellow_tripdata_2024-03.parquet' (FORMAT PARQUET)" >/dev/null
echo "planted (taxi): tip_amount + 1.00 where VendorID=$MV tpep_pickup_datetime=$MT PULocationID=$MP fare_amount=$MF"

for f in gh/*.parquet; do link "$f" gh_copy/; link "$f" gh_mut/; done
GMUT=$(duckdb -no-agent -csv -noheader -c "SET TimeZone='UTC'; SELECT repo_id || '|' || actor_id || '|' || strftime(created_at, '%Y-%m-%d %H:%M:%S')
  FROM read_parquet('gh/2024-01-01-05.parquet') GROUP BY repo_id, actor_id, created_at
  HAVING count(*) = 1 ORDER BY created_at, repo_id, actor_id LIMIT 1 OFFSET 1000")
IFS='|' read -r GR GA GT <<<"$GMUT"
rm gh_mut/2024-01-01-05.parquet
duckdb -no-agent -c "SET TimeZone='UTC'; COPY (SELECT * REPLACE (CASE WHEN repo_id = $GR AND actor_id = $GA
  AND created_at = TIMESTAMPTZ '$GT+00' THEN 'PlantedEvent' ELSE type END AS type)
  FROM read_parquet('gh/2024-01-01-05.parquet'))
  TO 'gh_mut/2024-01-01-05.parquet' (FORMAT PARQUET)" >/dev/null
echo "planted (gh): type = 'PlantedEvent' where repo_id=$GR actor_id=$GA created_at=$GT UTC"
# The planted keys as `reconcile` prints them; showcase.py fails if the
# differing key it reports is any other.
printf '{"rec-taxi-planted": ["%s", "%sZ", "%s", "%s"], "rec-gh-planted": ["%s", "%s", "%sZ"]}\n' \
  "$MV" "${MT/ /T}" "$MP" "$MF" "$GR" "$GA" "${GT/ /T}" >planted.json

# The market corpus is synthetic (seeded), written by the binary under test.
"$BW" bench market --out market --rung medium --seed 7 >/dev/null

# ---- measure, then render ----------------------------------------------------
python3 "$REPO/scripts/showcase.py" --bin "$BW" --work "$W" --build "$BUILD" --disk "$DISK" \
  --out "$REPO/docs/measured/results/showcase.json" "$@"
python3 "$REPO/scripts/render_charts.py"
echo "log: $W/out/showcase.log"
