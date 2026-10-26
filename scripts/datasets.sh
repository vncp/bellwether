# Public datasets shared by scripts/realdata.sh and scripts/showcase.sh:
# downloads verified against Content-Length (a complete file is never fetched
# again), and the GH Archive flattening. Source this file; it only defines
# functions. Needs curl and duckdb (2.x, run as `duckdb -no-agent`).

bw_size() { if [ -f "$1" ]; then wc -c <"$1" | tr -d ' '; else echo 0; fi; }

bw_fetch() { # bw_fetch URL DEST
  local want have
  want=$(curl -sSfI "$1" | tr -d '\r' | awk 'tolower($1)=="content-length:" {print $2}' | tail -1)
  have=$(bw_size "$2")
  [ -n "$want" ] && [ "$have" = "$want" ] && return 0
  curl -sSfL --retry 3 -o "$2.part" "$1" || { echo "download failed: $1" >&2; exit 1; }
  have=$(bw_size "$2.part")
  [ "$have" = "$want" ] || { echo "truncated: $1 ($have of $want bytes)" >&2; exit 1; }
  mv "$2.part" "$2"
}

# NYC TLC yellow trips for 2024, one file per month, as published.
bw_fetch_taxi() { # bw_fetch_taxi DL "01 02 ..."
  local m
  mkdir -p "$1/taxi"
  for m in $2; do
    bw_fetch "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-$m.parquet" \
      "$1/taxi/yellow_tripdata_2024-$m.parquet"
  done
}

# GH Archive hours of 2024-01-01 (nested JSON lines, gzipped).
bw_fetch_gh() { # bw_fetch_gh DL "0 1 ..."
  local h
  mkdir -p "$1/gh"
  for h in $2; do
    bw_fetch "https://data.gharchive.org/2024-01-01-$h.json.gz" "$1/gh/2024-01-01-$h.json.gz"
  done
}

# ClickBench `hits`, the athena_partitioned split (10,000,000 rows per 10 files).
bw_fetch_clickbench() { # bw_fetch_clickbench DL "0 1 ..."
  local i
  mkdir -p "$1/clickbench"
  for i in $2; do
    bw_fetch "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_$i.parquet" \
      "$1/clickbench/hits_$i.parquet"
  done
}

# GH Archive is nested JSON (repo.id, actor.id). Flattened by DuckDB to one
# Parquet file per hour, DuckDB's default writer settings, timestamps in UTC.
bw_flatten_gh() { # bw_flatten_gh DL OUTDIR "0 1 ..."
  local h f
  mkdir -p "$2"
  for h in $3; do
    f=$(printf '%02d' "$h")
    duckdb -no-agent -c "SET TimeZone='UTC'; COPY (SELECT repo.id AS repo_id, actor.id AS actor_id, type, created_at
      FROM read_json('$1/gh/2024-01-01-$h.json.gz', format='newline_delimited', maximum_object_size=100000000))
      TO '$2/2024-01-01-$f.parquet' (FORMAT PARQUET)" >/dev/null
  done
}
