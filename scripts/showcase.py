#!/usr/bin/env python3
"""End-to-end showcase: realistic questions on public data, three ways.

For every question this measures, on the same files:

  full scan       every row group of the dataset (compressed bytes from the
                  footers; what reading it all costs)
  duckdb          DuckDB answering the question in SQL with its own pruning
                  (min/max statistics, filter pushdown); bytes and row groups
                  from DuckDB's profiler (io.total_bytes_read,
                  total_row_groups_scanned), time from the profiler
  bellwether      `why` before any index (what the folder costs today and what
                  each candidate key would cost, by exact replay), then `fix`
                  (score, verify, apply, attest: a receipt), then the lookup
                  verb's plan on the new index (realized), then DuckDB running
                  the same SQL restricted to that plan's row groups, measured
                  like the plain DuckDB run. Both DuckDB answers are fingerprinted
                  (row count and an order-independent hash) and must be equal.

and, for reconciliation, `bellwether reconcile` against a DuckDB symmetric
EXCEPT ALL over the same two copies, with one planted change.

Writes one results file (docs/measured/results/README.md has the schema) and a
log of every command under --work/out. Called by scripts/showcase.sh, which
downloads and prepares the datasets; run that, not this.
"""

import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

REPEATS = 3  # timed runs per measured query; the median is recorded
TZ = "SET TimeZone='UTC';"


def log(out, line):
    print(line, flush=True)
    out.write(line + "\n")
    out.flush()


class Duck:
    def __init__(self, exe, logf):
        self.exe = exe
        self.logf = logf

    def rows(self, sql):
        """Run SQL and return the CSV rows as lists of strings."""
        p = subprocess.run(
            [self.exe, "-no-agent", "-csv", "-noheader", "-c", TZ + sql],
            capture_output=True,
            text=True,
        )
        if p.returncode != 0:
            sys.exit(f"duckdb failed ({p.returncode}): {p.stderr.strip()}\n{sql}")
        return [line.split(",") for line in p.stdout.splitlines() if line]

    def one(self, sql):
        return self.rows(sql)[0][0]

    def profile(self, sql, label, repeats=REPEATS):
        """Run `sql` REPEATS times under the JSON profiler.

        Returns bytes read, row groups scanned (and to scan), seconds (median),
        and the answer's fingerprint: row count and an order-independent hash
        (a sum, so two identical rows do not cancel as they would in an xor).
        """
        runs = []
        with tempfile.TemporaryDirectory() as tmp:
            prof = os.path.join(tmp, "profile.json")
            q = f"SELECT count(*), sum(hash(q)::HUGEINT) FROM ({sql}) q"
            for _ in range(repeats):
                setup = (
                    f"{TZ} SET enable_profiling='json'; SET profiling_output='{prof}';"
                    " SET tracked_metrics=['*'];"
                )
                p = subprocess.run(
                    [self.exe, "-no-agent", "-csv", "-noheader", "-c", setup + q + ";"],
                    capture_output=True,
                    text=True,
                )
                if p.returncode != 0:
                    sys.exit(f"duckdb failed ({p.returncode}): {p.stderr.strip()}\n{q}")
                n, h = p.stdout.strip().splitlines()[-1].split(",")
                d = json.load(open(prof, encoding="utf-8"))
                runs.append(
                    {
                        "bytes": d["io"]["total_bytes_read"],
                        "row_groups": d["query"]["total_row_groups_scanned"],
                        "row_groups_to_scan": d["query"]["total_row_groups_to_scan"],
                        "seconds": d["query"]["total_time"],
                        "rows": int(n),
                        "hash": h,
                    }
                )
        for r in runs[1:]:
            if (r["rows"], r["hash"]) != (runs[0]["rows"], runs[0]["hash"]):
                sys.exit(f"{label}: DuckDB returned different answers on repeated runs")
        mid = sorted(runs, key=lambda r: r["seconds"])[len(runs) // 2]
        self.logf.write(f"duckdb [{label}] {json.dumps(mid)}\n  {sql}\n")
        return mid


class Bw:
    def __init__(self, exe, logf):
        self.exe = exe
        self.logf = logf

    def run(self, args, ok=(0,)):
        t0 = time.perf_counter()
        p = subprocess.run([self.exe, *args, "--json"], capture_output=True, text=True)
        dt = time.perf_counter() - t0
        self.logf.write(f"$ bellwether {' '.join(args)} --json  (exit {p.returncode}, {dt:.3f} s)\n")
        if p.stderr.strip():
            self.logf.write(p.stderr.strip() + "\n")
        if p.returncode not in ok:
            sys.exit(f"bellwether {' '.join(args)} failed ({p.returncode}): {p.stderr.strip()}")
        v = json.loads(p.stdout) if p.stdout.strip() else None
        return v, dt, p.returncode

    def timed(self, args):
        """REPEATS runs of a read-only verb; its result and the median seconds."""
        out = [self.run(args) for _ in range(REPEATS)]
        return out[0][0], statistics.median(o[1] for o in out)


def lit(values, sql_type):
    if sql_type == "VARCHAR":
        return ", ".join("'" + v.replace("'", "''") + "'" for v in values)
    return ", ".join(values)


def ts_lit(instant, sql_type):
    # Bellwether takes RFC 3339 instants; DuckDB (TimeZone UTC) the same instant.
    return f"'{instant.replace('T', ' ').rstrip('Z')}'::{sql_type}"


# ---- the questions ------------------------------------------------------------
# kind: locate (rows holding a key), window (locate within a time window),
# latest (each key's newest version), asof (the version current at an instant),
# bitemporal (valid at V as known at K: no lookahead).
QUESTIONS = [
    {
        "id": "taxi-zones-week",
        "dataset": "taxi",
        "text": "trips from 10 pickup zones in one week",
        "noun": "trips",
        "keys": ("PULocationID", 10),
        "kind": "window",
        "window": ("2024-03-04T00:00:00Z", "2024-03-10T23:59:59Z"),
    },
    {
        "id": "taxi-latest-zone",
        "dataset": "taxi",
        "text": "the latest trip from every pickup zone",
        "noun": "trips",
        "keys": ("PULocationID", None),
        "kind": "latest",
    },
    {
        "id": "taxi-asof-zone",
        "dataset": "taxi",
        "text": "each pickup zone's last trip before 1 March",
        "noun": "trips",
        "keys": ("PULocationID", None),
        "kind": "asof",
        "at": "2024-03-01T00:00:00Z",
    },
    {
        "id": "taxi-zones-all",
        "dataset": "taxi",
        "text": "trips from 10 pickup zones, all six months",
        "noun": "trips",
        "keys": ("PULocationID", 10),
        "kind": "locate",
    },
    {
        "id": "gh-repos10",
        "dataset": "gh",
        "text": "events for 10 repositories",
        "noun": "events",
        "keys": ("repo_id", 10),
        "kind": "locate",
    },
    {
        "id": "gh-repos10-window",
        "dataset": "gh",
        "text": "events for 10 repositories, 06:00 to 08:00",
        "noun": "events",
        "keys": ("repo_id", 10),
        "kind": "window",
        "window": ("2024-01-01T06:00:00Z", "2024-01-01T07:59:59Z"),
    },
    {
        "id": "gh-latest10",
        "dataset": "gh",
        "text": "the latest event of 10 repositories",
        "noun": "events",
        "keys": ("repo_id", 10),
        "kind": "latest",
    },
    {
        "id": "gh-repos1000",
        "dataset": "gh",
        "text": "events for 1,000 repositories",
        "noun": "events",
        "keys": ("repo_id", 1000),
        "kind": "locate",
    },
    {
        "id": "cb-users10",
        "dataset": "clickbench",
        "text": "page views of 10 users",
        "noun": "page views",
        "keys": ("UserID", 10),
        "kind": "locate",
    },
    {
        "id": "cb-users1000",
        "dataset": "clickbench",
        "text": "page views of 1,000 users",
        "noun": "page views",
        "keys": ("UserID", 1000),
        "kind": "locate",
    },
    {
        "id": "cb-counters3",
        "dataset": "clickbench",
        "text": "page views of 3 sites (CounterID)",
        "noun": "page views",
        "keys": ("CounterID", 3),
        "kind": "locate",
    },
    {
        "id": "mkt-window",
        "dataset": "market",
        "text": "prints of 6 symbols in one hour",
        "noun": "prints",
        "keys": ("symbol", 6),
        "kind": "window",
        "window": ("2026-01-15T15:00:00Z", "2026-01-15T15:59:59Z"),
    },
    {
        "id": "mkt-latest",
        "dataset": "market",
        "text": "the latest print of 6 symbols",
        "noun": "prints",
        "keys": ("symbol", 6),
        "kind": "latest",
    },
    {
        "id": "mkt-asof-known",
        "dataset": "market",
        "text": "6 symbols at 16:00 as known that evening (no lookahead)",
        "noun": "prints",
        "keys": ("symbol", 6),
        "kind": "bitemporal",
        "valid": "2026-01-15T16:00:00Z",
        "known": "2026-01-16T00:00:00Z",
    },
]

DATASETS = {
    "taxi": {
        "dir": "taxi",
        "glob": "*.parquet",
        "name": "NYC taxi",
        "label": "NYC yellow taxi trips, 2024-01 to 2024-06 (public)",
        "ts": "tpep_pickup_datetime",
        "ts_type": "TIMESTAMP",
        "hive": False,
    },
    "gh": {
        "dir": "gh",
        "glob": "*.parquet",
        "name": "GH Archive",
        "label": "GH Archive events, 2024-01-01 00:00 to 11:59 UTC, flattened (public)",
        "ts": "created_at",
        "ts_type": "TIMESTAMPTZ",
        "hive": False,
    },
    "clickbench": {
        "dir": "clickbench",
        "glob": "*.parquet",
        "name": "ClickBench",
        "label": "ClickBench hits, athena_partitioned hits_0..9 (public)",
        "ts": None,
        "ts_type": None,
        "hive": False,
    },
    "market": {
        "dir": "market/data",
        "glob": "*/*.parquet",
        "name": "Market (synthetic)",
        "label": "market prints, synthetic (`bellwether bench market --rung medium --seed 7`)",
        "ts": "ts",
        "ts_type": "TIMESTAMPTZ",
        "known": "known_at",
        # Hive partitioning (`day=` directories) only with --market-hive:
        # `fix` does not yet record --partitioning in the index it builds.
        # The questions never filter on `day`, so the bytes are the same.
        "hive": False,
    },
}


class Showcase:
    def __init__(self, a, logf):
        self.a = a
        self.logf = logf
        self.duck = Duck(a.duckdb, logf)
        self.bw = Bw(a.bin, logf)
        self.work = os.path.realpath(a.work)
        self.rows = []
        self.queries = {}
        self.meta_cache = {}
        self.dataset_cost = {}
        planted = os.path.join(self.work, "planted.json")
        self.planted = json.load(open(planted, encoding="utf-8")) if os.path.exists(planted) else {}

    # -- results rows ----------------------------------------------------------
    def row(self, dataset, query, side, metric, value, unit, exactness):
        build = self.a.duckdb_build if side.startswith("duckdb") else self.a.build
        if side == "full-scan":
            build = "footer statistics (no engine)"
        self.rows.append(
            {
                "dataset": dataset,
                "query": query,
                "side": side,
                "metric": metric,
                "value": value,
                "unit": unit,
                "exactness": exactness,
                "machine": self.a.machine,
                "build": build,
                "date": self.a.date,
                # Other work on the machine skews timings, never bytes.
                "notes": f"1-minute load average {os.getloadavg()[0]:.1f} on {os.cpu_count()} cores"
                if exactness == "measured"
                else "",
            }
        )

    # -- datasets --------------------------------------------------------------
    def root(self, ds):
        return os.path.join(self.work, DATASETS[ds]["dir"])

    def src(self, ds, files=None):
        d = DATASETS[ds]
        target = (
            "[" + ", ".join(f"'{f}'" for f in files) + "]"
            if files is not None
            else f"'{self.root(ds)}/{d['glob']}'"
        )
        hive = ", hive_partitioning=true" if d["hive"] else ", hive_partitioning=false"
        return target, hive

    def bw_args(self, ds):
        return ["--partitioning", "hive"] if DATASETS[ds]["hive"] else []

    def row_ranges(self, ds):
        """Per file: (first row, rows) of every row group, from the footers."""
        if ds in self.meta_cache:
            return self.meta_cache[ds]
        target, _ = self.src(ds)
        out = {}
        for f, rg, n in self.duck.rows(
            f"SELECT DISTINCT file_name, row_group_id, row_group_num_rows FROM parquet_metadata({target})"
            " ORDER BY file_name, row_group_id"
        ):
            out.setdefault(f, []).append((int(rg), int(n)))
        ranges = {}
        for f, rgs in out.items():
            lo, acc = 0, {}
            for rg, n in sorted(rgs):
                acc[rg] = (lo, n)
                lo += n
            ranges[f] = acc
        self.meta_cache[ds] = ranges
        return ranges

    # -- keys ------------------------------------------------------------------
    def keys(self, q):
        col, n = q["keys"]
        ds = q["dataset"]
        path = os.path.join(self.work, "keys", f"{q['id']}.parquet")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        target, hive = self.src(ds)
        limit = f"LIMIT {n}" if n else ""
        # Distinct values chosen by a seeded hash, so the choice does not depend
        # on DuckDB's thread scheduling.
        self.duck.rows(
            f"COPY (SELECT {col} FROM (SELECT DISTINCT {col} FROM read_parquet({target}{hive})"
            f" WHERE {col} IS NOT NULL) ORDER BY hash({col}, 42), {col} {limit}) TO '{path}' (FORMAT PARQUET)"
        )
        sql_type = self.duck.one(f"SELECT typeof({col}) FROM '{path}' LIMIT 1")
        values = [r[0] for r in self.duck.rows(f"SELECT {col} FROM '{path}' ORDER BY 1")]
        return path, values, "VARCHAR" if sql_type == "VARCHAR" else sql_type

    # -- SQL -------------------------------------------------------------------
    def sql_parts(self, q, values, sql_type):
        d = DATASETS[q["dataset"]]
        col = q["keys"][0]
        where = [f"{col} IN ({lit(values, sql_type)})"]
        qualify = None
        ts, tt = d["ts"], d["ts_type"]
        k = q["kind"]
        if k == "window":
            a, b = q["window"]
            where.append(f"{ts} BETWEEN {ts_lit(a, tt)} AND {ts_lit(b, tt)}")
        elif k == "latest":
            where.append(f"{ts} IS NOT NULL")
            qualify = f"rank() OVER (PARTITION BY {col} ORDER BY {ts} DESC) = 1"
        elif k == "asof":
            where.append(f"{ts} <= {ts_lit(q['at'], tt)}")
            qualify = f"rank() OVER (PARTITION BY {col} ORDER BY {ts} DESC) = 1"
        elif k == "bitemporal":
            kn = d["known"]
            where.append(f"{ts} <= {ts_lit(q['valid'], tt)}")
            where.append(f"{kn} <= {ts_lit(q['known'], tt)}")
            qualify = f"rank() OVER (PARTITION BY {col} ORDER BY {ts} DESC, {kn} DESC) = 1"
        return " AND ".join(where), qualify

    def sql_all(self, q, where, qualify):
        target, hive = self.src(q["dataset"])
        s = f"SELECT * FROM read_parquet({target}{hive}) WHERE {where}"
        return s + (f" QUALIFY {qualify}" if qualify else "")

    def sql_plan(self, q, plan, where, qualify):
        """The same SQL, restricted to the plan's row groups by row number."""
        ranges = self.row_ranges(q["dataset"])
        _, hive = self.src(q["dataset"])
        parts = []
        for f in sorted(plan):
            spans = []
            for rg in sorted(plan[f]):
                lo, n = ranges[f][rg]
                if spans and spans[-1][1] + 1 == lo:
                    spans[-1][1] = lo + n - 1
                else:
                    spans.append([lo, lo + n - 1])
            rn = " OR ".join(f"file_row_number BETWEEN {a} AND {b}" for a, b in spans)
            parts.append(
                f"SELECT * EXCLUDE (file_row_number) FROM read_parquet('{f}'{hive}, file_row_number=true)"
                f" WHERE ({rn}) AND {where}"
            )
        if not parts:
            return None
        s = " UNION ALL ".join(parts)
        return f"SELECT * FROM ({s})" + (f" QUALIFY {qualify}" if qualify else "")

    # -- one question ------------------------------------------------------------
    def question(self, q):
        ds, qid, k = q["dataset"], q["id"], q["kind"]
        d = DATASETS[ds]
        root = self.root(ds)
        idx = os.path.join(self.work, "idx", qid)
        shutil.rmtree(idx, ignore_errors=True)
        common = ["--index", idx, *self.bw_args(ds)]
        log(self.logf, f"\n## {qid}: {q['text']}")
        keys, values, sql_type = self.keys(q)
        col = q["keys"][0]
        where, qualify = self.sql_parts(q, values, sql_type)

        # why: the funnel before any index, with each candidate priced by replay.
        mode = []
        if k == "window":
            mode = ["--window", f"{q['window'][0]}..{q['window'][1]}", "--ts", d["ts"]]
        elif k == "latest":
            mode = ["--latest-by", d["ts"]]
        elif k == "asof":
            mode = ["--latest-by", d["ts"], "--at", q["at"]]
        elif k == "bitemporal":
            # `why` prices valid-time lookups; the bitemporal one is not priced.
            mode = ["--latest-by", d["ts"], "--at", q["valid"]]
        why, why_s, _ = self.bw.run(["why", root, *common, "--from", keys, *mode])
        dataset, current = why["dataset"], why["current"]
        if ds not in self.dataset_cost:
            target, _ = self.src(ds)
            chunks, minmax = self.duck.rows(
                f"SELECT count(*), count(stats_min_value) FROM parquet_metadata({target})"
            )[0]
            self.dataset_cost[ds] = {**dataset, "column_chunks": int(chunks), "column_chunks_with_minmax": int(minmax)}
        self.row(ds, qid, "full-scan", "bytes", dataset["bytes"], "bytes", "exact")
        self.row(ds, qid, "full-scan", "row_groups", dataset["row_groups"], "row groups", "exact")
        self.row(ds, qid, "bellwether-unindexed", "bytes", current["bytes"], "bytes", "exact")
        self.row(ds, qid, "bellwether-unindexed", "row_groups", current["row_groups"], "row groups", "exact")
        self.row(ds, qid, "bellwether-why", "wall_ms", round(why_s * 1000, 1), "ms", "measured")

        # The key to fix: why's recommendation, else the cheapest candidate (the
        # floor), so a key that cannot help is still put to `fix` and refused.
        rec = why.get("recommended")
        if rec:
            spec, proposal, predicted = rec["spec"], rec["proposal"], rec["proposed"]
        else:
            best = min(why["candidates"], key=lambda c: c["proposed"]["bytes"])
            spec, proposal, predicted = best["spec"], best["proposal"]["hash"], best["proposed"]
        spec_s = spec_text(spec)
        self.row(ds, qid, "bellwether", "recommended", spec_s if rec else "none", "key spec", "exact")
        if k == "bitemporal":
            # Bitemporal keys cannot be proposed or receipted yet (planned with
            # storage version 2): this one is built with `index`, no receipt.
            spec_s = f"temporal2:{col}@{d['ts']},{d['known']}"
            self.bw.run(["index", root, *common, "--key", spec_s])
            self.row(ds, qid, "bellwether", "fix", "not run: bitemporal keys are built with index (no receipt until storage v2)", "outcome", "exact")
            self.row(ds, qid, "bellwether", "key", spec_s, "key spec", "exact")
            applied = True
        else:
            fix, fix_s, code = self.bw.run(
                ["fix", root, *common, "--proposal", proposal, "--workload", why["workload"]["hash"]],
                ok=(0, 3),
            )
            self.row(ds, qid, "bellwether-predicted", "bytes", predicted["bytes"], "bytes", "exact")
            self.row(ds, qid, "bellwether-predicted", "row_groups", predicted["row_groups"], "row groups", "exact")
            self.row(ds, qid, "bellwether", "key", spec_s, "key spec", "exact")
            self.row(ds, qid, "bellwether-fix", "wall_ms", round(fix_s * 1000, 1), "ms", "measured")
            outcome = (fix or {}).get("outcome") or {}
            applied = code == 0 and outcome.get("kind") == "applied"
            if applied:
                self.row(ds, qid, "bellwether", "fix", "applied", "outcome", "exact")
                self.row(ds, qid, "bellwether", "receipt", outcome["receipt"]["hash"], "blake3", "identity")
                realized = outcome["realized"]["record"]["current"][0]
                self.row(ds, qid, "bellwether-realized", "bytes", realized["bytes"], "bytes", "exact")
                self.row(ds, qid, "bellwether-realized", "row_groups", realized["row_groups"], "row groups", "exact")
            else:
                refusal = outcome.get("refusal") or {}
                reason = refusal.get("kind") or outcome.get("kind") or f"exit {code}"
                self.row(ds, qid, "bellwether", "fix", f"refused: {reason}", "outcome", "exact")

        # The lookup on the new index: the plan an engine would follow.
        if applied:
            if k == "window":
                look = ["locate", "--window", f"{q['window'][0]}..{q['window'][1]}", "--ts", d["ts"]]
            elif k == "asof":
                look = ["asof", "--at", q["at"]]
            elif k == "bitemporal":
                look = ["asof", "--valid", q["valid"], "--known", q["known"]]
            else:
                look = [k]
            v, look_s = self.bw.timed([look[0], root, *common, "--keys", keys, *look[1:]])
            planned = v["planned"]
            plan = {}
            for e in v["evidence"]["record"]["body"]["plan"]["entries"]:
                plan.setdefault(os.path.realpath(e["file"]["path"]), set()).update(e["row_groups"])
            self.row(ds, qid, "bellwether-fixed", "bytes", planned["bytes"], "bytes", "exact")
            self.row(ds, qid, "bellwether-fixed", "row_groups", planned["row_groups"], "row groups", "exact")
            self.row(ds, qid, "bellwether-plan", "wall_ms", round(look_s * 1000, 1), "ms", "measured")
            plan_sql = self.sql_plan(q, plan, where, qualify)
        all_sql = self.sql_all(q, where, qualify)

        # DuckDB alone, then (when `fix` left an index) DuckDB on the plan;
        # the two answers must agree. A refused fix leaves no index and no
        # plan, so there is nothing to run on a plan and no row for one.
        alone = self.duck.profile(all_sql, f"{qid} duckdb")
        sides = [("duckdb", alone)]
        if applied:
            if plan_sql is None:  # the plan holds no row group
                on_plan = {"bytes": 0, "row_groups": 0, "seconds": 0.0, "rows": 0, "hash": alone["hash"] if alone["rows"] == 0 else "none"}
            else:
                on_plan = self.duck.profile(plan_sql, f"{qid} plan")
            if (alone["rows"], alone["hash"]) != (on_plan["rows"], on_plan["hash"]):
                sys.exit(
                    f"{qid}: DuckDB on Bellwether's plan returned a different answer "
                    f"({on_plan['rows']} rows) than DuckDB on every file ({alone['rows']} rows)"
                )
            sides.append(("duckdb-on-plan", on_plan))
        for side, m in sides:
            self.row(ds, qid, side, "bytes", m["bytes"], "bytes", "measured")
            self.row(ds, qid, side, "row_groups", m["row_groups"], "row groups", "measured")
            self.row(ds, qid, side, "wall_ms", round(m["seconds"] * 1000, 1), "ms", "measured")
        self.row(ds, qid, "duckdb", "rows_returned", alone["rows"], "rows", "exact")
        if applied:
            self.row(ds, qid, "duckdb-on-plan", "same_answer", True, "bool", "exact")
            self.row(
                ds, qid, "bellwether+duckdb", "wall_ms",
                round((look_s + on_plan["seconds"]) * 1000, 1), "ms", "measured",
            )
        self.queries[qid] = {
            "dataset": ds,
            "text": q["text"],
            "noun": q["noun"],
            "kind": k,
            "keys": f"{len(values)} {col} values",
            "sql": self.portable(all_sql if len(all_sql) < 600 else all_sql[:280] + " ... " + all_sql[-200:]),
        }
        log(
            self.logf,
            f"  rows {alone['rows']}; full {dataset['bytes']}; duckdb {alone['bytes']} ({alone['row_groups']} rg); "
            f"bellwether unindexed {current['bytes']}, fixed {self.last('bytes', qid, 'bellwether-fixed')}, "
            f"on plan {self.last('bytes', qid, 'duckdb-on-plan')} ({self.last('row_groups', qid, 'duckdb-on-plan')} rg)",
        )

    def portable(self, sql):
        """The SQL with the scratch directory written as <work>, so the
        results file names no local path."""
        return sql.replace(self.work + "/", "<work>/").replace(os.path.realpath(self.work) + "/", "<work>/")

    def last(self, metric, qid, side):
        for r in reversed(self.rows):
            if r["query"] == qid and r["side"] == side and r["metric"] == metric:
                return r["value"]
        return None

    # -- reconcile -------------------------------------------------------------
    def reconcile(self, rid, ds, left, right, key, text, planted):
        log(self.logf, f"\n## {rid}: {text}")
        L, R = os.path.join(self.work, left), os.path.join(self.work, right)
        idx = os.path.join(self.work, "idx", rid)
        shutil.rmtree(idx, ignore_errors=True)
        v, secs, code = self.bw.run(["reconcile", L, R, "--index", idx, "--key", key], ok=(0, 1))
        o = v["outcome"]
        c = o["counts"]
        rows = sum(s["rows"] for s in o["sides"])
        found = c["mismatched"] + c["missing_left"] + c["missing_right"]
        self.row(ds, rid, "bellwether", "rows_per_s", round(rows / secs), "rows/s", "measured")
        self.row(ds, rid, "bellwether", "wall_ms", round(secs * 1000, 1), "ms", "measured")
        self.row(ds, rid, "bellwether", "rows_compared", rows, "rows", "exact")
        self.row(ds, rid, "bellwether", "differing_keys", found, "keys", "exact")
        self.row(ds, rid, "bellwether", "passes", o["memory"]["passes"], "passes", "exact")
        self.row(ds, rid, "bellwether", "certificate", v["certificate"]["hash"], "blake3", "identity")
        self.row(ds, rid, "bellwether", "pass", bool(o["pass"]), "bool", "exact")
        if planted:
            # The key the --json output names (the certificate holds counts
            # only); it must be the one the script planted.
            if rid not in self.planted:
                sys.exit(
                    f"{rid}: {os.path.join(self.work, 'planted.json')} does not name the planted key; "
                    "run scripts/showcase.sh, which plants the change and writes that file"
                )
            want = self.planted[rid]
            got = [m["key"]["values"] for m in o["mismatches"]]
            if got != [want]:
                sys.exit(f"{rid}: reconcile reported differing keys {got}, but the planted key is {want}")
            self.row(ds, rid, "bellwether", "differing_key", ", ".join(got[0]), "key", "exact")
            self.row(ds, rid, "bellwether", "found_planted", True, "bool", "exact")
        a = f"read_parquet('{L}/*.parquet')"
        b = f"read_parquet('{R}/*.parquet')"
        diff = (
            f"SELECT * FROM ((SELECT *, 'left' AS side FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b}))"
            f" UNION ALL (SELECT *, 'right' AS side FROM (SELECT * FROM {b} EXCEPT ALL SELECT * FROM {a})))"
        )
        # One run each side, like `bellwether reconcile` (which writes a
        # certificate per run): both read every row of both copies.
        m = self.duck.profile(diff, f"{rid} duckdb", repeats=1)
        self.row(ds, rid, "duckdb", "rows_per_s", round(rows / m["seconds"]), "rows/s", "measured")
        self.row(ds, rid, "duckdb", "wall_ms", round(m["seconds"] * 1000, 1), "ms", "measured")
        self.row(ds, rid, "duckdb", "rows_compared", rows, "rows", "exact")
        self.row(ds, rid, "duckdb", "differing_rows", m["rows"], "rows", "exact")
        self.queries[rid] = {
            "dataset": ds,
            "text": text,
            "kind": "reconcile",
            "keys": f"key ({key})",
            "planted": planted,
            "sql": self.portable(diff),
        }
        log(
            self.logf,
            f"  bellwether {secs:.1f} s, {found} differing keys, pass={o['pass']}, passes {o['memory']['passes']};"
            f" duckdb {m['seconds']:.1f} s, {m['rows']} differing rows",
        )


def spec_text(spec):
    cols = ",".join(spec["columns"])
    k = spec["kind"]
    if k == "temporal":
        return f"temporal:{cols}@{spec['ts_column']}"
    if k == "bloom":
        return f"bloom:{cols}@{spec.get('fpp', '')}"
    if spec.get("ts_column"):
        return f"{k}:{cols}@{spec['ts_column']}"
    return f"{k}:{cols}"


def machine():
    if platform.system() == "Darwin":
        sc = lambda k: subprocess.run(["sysctl", "-n", k], capture_output=True, text=True).stdout.strip()
        ver = subprocess.run(["sw_vers", "-productVersion"], capture_output=True, text=True).stdout.strip()
        return f"{sc('machdep.cpu.brand_string')}, {sc('hw.ncpu')} cores, {int(sc('hw.memsize')) // 2**30} GB, macOS {ver}"
    mem = int(open("/proc/meminfo").read().split("MemTotal:")[1].split()[0]) // 2**20
    return f"{platform.machine()}, {os.cpu_count()} cores, {mem} GB, {platform.system()} {platform.release()}"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bin", required=True, help="the bellwether binary (a release build)")
    ap.add_argument("--work", required=True, help="the prepared datasets (scripts/showcase.sh)")
    ap.add_argument("--out", required=True, help="the results file to write")
    ap.add_argument("--duckdb", default="duckdb")
    ap.add_argument("--build", required=True, help="how the binary was built, for every row")
    ap.add_argument("--disk", default="local SSD")
    ap.add_argument("--only", help="comma-separated question or reconcile ids (debugging)")
    ap.add_argument(
        "--accept-new-inputs",
        action="store_true",
        help="measure even though DuckDB's version or a downloaded file differs from what the results file records",
    )
    ap.add_argument(
        "--market-hive",
        action="store_true",
        help="read the market corpus with hive partitioning (`day=` directories) on every side; needs an "
        "engine whose `fix` records --partitioning in the index it builds",
    )
    a = ap.parse_args()
    DATASETS["market"]["hive"] = a.market_hive
    a.machine = f"{machine()}, {a.disk}"
    a.date = time.strftime("%Y-%m-%d", time.gmtime())
    dv = subprocess.run([a.duckdb, "-no-agent", "--version"], capture_output=True, text=True).stdout.split()
    a.duckdb_build = f"DuckDB {dv[0]} ({dv[-1]})" if dv else "DuckDB"
    only = set(a.only.split(",")) if a.only else None
    a.downloads = read_downloads(os.path.join(a.work, "downloads.sha256"))
    pin(a)
    os.makedirs(os.path.join(a.work, "out"), exist_ok=True)
    with open(os.path.join(a.work, "out", "showcase.log"), "w", encoding="utf-8") as logf:
        s = Showcase(a, logf)
        log(logf, f"machine: {a.machine}\nbellwether: {a.build}\nduckdb: {a.duckdb_build}\ndate: {a.date}")
        for q in QUESTIONS:
            if only is None or q["id"] in only:
                s.question(q)
                save(a.out, s)
        for r in RECONCILES:
            if only is None or r[0] in only:
                s.reconcile(*r)
                save(a.out, s)


RECONCILES = [
    ("rec-taxi-same", "taxi", "taxi", "taxi_duck", "VendorID,tpep_pickup_datetime,PULocationID,fare_amount",
     "taxi trips against DuckDB's rewrite of the same files", None),
    ("rec-taxi-planted", "taxi", "taxi", "taxi_duck_mut", "VendorID,tpep_pickup_datetime,PULocationID,fare_amount",
     "taxi trips against the rewrite with one tip changed", "tip_amount + 1.00 on one March trip"),
    ("rec-gh-same", "gh", "gh", "gh_copy", "repo_id,actor_id,created_at",
     "GH events against a copy", None),
    ("rec-gh-planted", "gh", "gh", "gh_mut", "repo_id,actor_id,created_at",
     "GH events against a copy with one event type changed", "type of one event changed"),
]


def read_downloads(path):
    """{file: sha256} from the `sha256sum` listing showcase.sh writes."""
    if not os.path.exists(path):
        return {}
    out = {}
    for line in open(path, encoding="utf-8"):
        digest, name = line.split(maxsplit=1)
        out[name.strip().lstrip("*")] = digest
    return out


def pin(a):
    """Refuse to mix inputs: the DuckDB version decides the GH Parquet layout,
    the sampled keys, and the taxi rewrite, and the downloads are the data.
    Both must match what the results file already records."""
    if a.accept_new_inputs:
        if a.only:
            sys.exit(
                "--accept-new-inputs re-measures on new inputs, so it measures everything: drop --only "
                "(rows measured on the old inputs would otherwise stay in the results file)"
            )
        return
    if not os.path.exists(a.out):
        return
    old = json.load(open(a.out, encoding="utf-8"))
    builds = {r["build"] for r in old["rows"] if r["side"].startswith("duckdb")}
    if builds and builds != {a.duckdb_build}:
        sys.exit(
            f"{a.out} was measured with {', '.join(sorted(builds))}, this DuckDB is {a.duckdb_build}: "
            "install that version, or pass --accept-new-inputs and re-measure everything (no --only)"
        )
    recorded = old.get("downloads") or {}
    changed = sorted(f for f in recorded.keys() & a.downloads.keys() if recorded[f] != a.downloads[f])
    if changed:
        sys.exit(
            f"downloaded files differ from the ones {a.out} was measured on: {', '.join(changed)}; "
            "pass --accept-new-inputs and re-measure everything (no --only)"
        )


def save(path, s):
    """Write the results file after every question: the ids measured in this
    run replace their old rows; the others are kept, in the canonical order,
    and ids no longer asked are dropped."""
    order = [q["id"] for q in QUESTIONS] + [r[0] for r in RECONCILES]
    old = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else {"datasets": {}, "queries": {}, "rows": []}
    ids = set(s.queries)
    datasets = {}
    for k, v in DATASETS.items():
        prev = old.get("datasets", {}).get(k)
        cost = s.dataset_cost.get(k) or (
            {f: v for f, v in prev.items() if f not in ("name", "label")} if isinstance(prev, dict) else {}
        )
        datasets[k] = {"name": v["name"], "label": v["label"], **cost}
    merged = {**{k: v for k, v in old["queries"].items() if k not in ids}, **s.queries}
    rows = [r for r in old["rows"] if r["query"] not in ids] + s.rows
    doc = {
        "source": "showcase",
        "generated_by": "scripts/showcase.sh",
        "datasets": datasets,
        "downloads": s.a.downloads or old.get("downloads", {}),
        "queries": {k: merged[k] for k in order if k in merged},
        "rows": sorted((r for r in rows if r["query"] in order), key=lambda r: order.index(r["query"])),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1)
        f.write("\n")
    print(f"wrote {path}: {len(doc['rows'])} rows")


if __name__ == "__main__":
    main()
