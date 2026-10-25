#!/usr/bin/env bash
# Release measurements for WP22: lookups against a full scan and DuckDB,
# build scaling, reconcile throughput, replay and observe latency, resident
# bytes per key, and samply profiles of the hot paths. Writes
# benches/out/perf.json (format bellwether-perf/1) and benches/out/profiles/;
# docs/measured/*-performance.md is written from them.
#
#   scripts/perf.sh                       # everything (about an hour on a laptop)
#   scripts/perf.sh --ci                  # small corpora only, no profiles
#   scripts/perf.sh --sections lookups    # one section; see scripts/perf.py --help
#   PERF_WORK=/scratch REALDATA_WORK=$REALDATA_DIR/work scripts/perf.sh
#
# Needs: cargo, python3, duckdb (2.x, run as `duckdb -no-agent`), and for
# profiles samply (`cargo install samply`). REALDATA_WORK is the work
# directory of scripts/realdata.sh (gh/, taxi/, taxi_duck/); without it the
# GH events lookups and the taxi reconcile are skipped and say so.
#
# Builds the release binary in a private target dir
# ($HOME/.cache/bellwether-target-wp22-release unless CARGO_TARGET_DIR is
# set), with line-table debug info so the profiles have names. Generates
# corpora under $PERF_WORK (default $TMPDIR/bellwether-perf, about 7 GB with
# the small rung and its copy). Deletes nothing outside $PERF_WORK; remove it
# and the target dir when done. Cold samples drop the OS page cache only when
# `sudo -n` allows it; otherwise perf.json labels them process-cold.

set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
WORK=${PERF_WORK:-${TMPDIR:-/tmp}/bellwether-perf}

free_gb=$(df -Pk / | awk 'NR==2 {print int($4 / 1048576)}')
if [ "$free_gb" -lt 8 ]; then
  echo "only ${free_gb} GB free on /; stopping (need 8)" >&2
  exit 1
fi
for tool in cargo python3 duckdb; do
  command -v "$tool" >/dev/null || { echo "perf.sh needs $tool on PATH" >&2; exit 1; }
done

export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/bellwether-target-wp22-release}
export CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
(cd "$REPO" && cargo build --release -p bellwether-cli)

exec python3 "$REPO/scripts/perf.py" --bin "$CARGO_TARGET_DIR/release/bellwether" --work "$WORK" "$@"
