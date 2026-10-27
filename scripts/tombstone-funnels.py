#!/usr/bin/env python3
"""Measured funnels for tombstones (B8) on both corpus shapes.

The operator is `arg_max(ts, *) by key` followed by `where not retracted`:
each key's newest row (at or before an instant), kept only when it is not a
retraction. Two corpora from `bellwether bench tombstone`:

- the ledger with voids (`--shape ledger`): `order_id`, `ts`, `amount`,
  `void`, 20% of rows voids and 0.1% of `void` null, tombstone `void`;
- the market with retractions (`--shape market`): `trade_id`, `symbol`,
  `ts`, `price`, `size`, `status`, 5% of prints `CANCEL`, tombstone
  `status=CANCEL`.

Each is indexed first with `temporal:<key>@ts` (no tombstone: the lookup
is `asof` plus the filter), priced with `why --latest-by ts --tombstone
<t>` (the `tombstone` column and the R-tombstone-1 lint), then indexed with
`temporal:<key>@ts,tombstone=<t>`, which builds the tombstone sidecar.

For each run (10, 10^3, 10^5 keys) and lookup (`latest`, and `asof` at the
workload's instant) the table reports, from the CLI's --json output (exact
sums of statistics, never estimates):

  keys with a version   at the instant, and of them those whose newest row
                        is not live (a retraction, or a null `void`)
  asof + filter         the row groups and compressed bytes `latest`/`asof`
                        plans without --live: every key's newest row, which
                        the filter then drops for the retracted keys
  asof --live           the same lookup with --live: row groups and bytes
                        it plans, which sidecar pruned it, and the keys it
                        reports retracted
  oracle                the row groups holding each key's newest row, and
                        those holding a live newest row, decoded from the
                        files with pyarrow (independently of the index and
                        of the generator's truth.json, which they also
                        equal); "plans = oracle" says both plans are
                        exactly those sets

Usage: scripts/tombstone-funnels.py --bin PATH/TO/bellwether [--work DIR]
       [--label "debug build"] [--small]

--small writes 2 x 10^5 rows per shape instead of 2 x 10^6. Needs python3
with pyarrow. Prints markdown; the label line names the machine and the
build (ADR 019). No timing is measured.
"""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile

import pyarrow.parquet as pq

SHAPES = ["ledger", "market"]


def run(bin_, args):
    out = subprocess.run([bin_, *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"bellwether {' '.join(args)} failed ({out.returncode}): {out.stderr.strip()}")
    return out.stdout


def js(bin_, args):
    return json.loads(run(bin_, [*args, "--json"]))


def machine():
    cpu = platform.processor() or platform.machine()
    if sys.platform == "darwin":
        try:
            cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                 capture_output=True, text=True).stdout.strip() or cpu
        except OSError:
            pass
    return f"{platform.system()} {platform.release()} {platform.machine()}, {cpu}, {os.cpu_count()} cores"


def commit():
    repo = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    out = subprocess.run(["git", "-C", repo, "rev-parse", "--short", "HEAD"],
                         capture_output=True, text=True)
    return out.stdout.strip() or "unknown"


def decode(data, key, tomb_col, tomb_value):
    """Per key: its rows as (ts, (file, row group), live), decoded one row
    group at a time. A null tombstone column is not live (ADR 014)."""
    rows = {}
    for name in sorted(f for f in os.listdir(data) if f.endswith(".parquet")):
        pf = pq.ParquetFile(os.path.join(data, name))
        for g in range(pf.num_row_groups):
            t = pf.read_row_group(g, columns=[key, "ts", tomb_col])
            ks = t.column(key).to_pylist()
            ts = t.column("ts").cast("int64").to_pylist()
            marks = t.column(tomb_col).to_pylist()
            for k, s, m in zip(ks, ts, marks):
                live = m is not None and m != tomb_value
                rows.setdefault(k, []).append((s, (name, g), live))
    return rows


def oracle(rows, keys, at):
    """The row groups holding each key's newest row at or before `at`
    (`None`: latest), those holding a live one, and the keys with a version
    and those whose newest row is not live."""
    newest_rg, live_rg = set(), set()
    with_version = retracted = 0
    for k in keys:
        cands = [r for r in rows.get(k, []) if at is None or r[0] <= at]
        if not cands:
            continue
        with_version += 1
        top = max(r[0] for r in cands)
        newest = [r for r in cands if r[0] == top]
        newest_rg.update(r[1] for r in newest)
        lives = [r for r in newest if r[2]]
        live_rg.update(r[1] for r in lives)
        if not lives:
            retracted += 1
    return newest_rg, live_rg, with_version, retracted


def plan_row_groups(plan, data):
    base = os.path.realpath(data) + "/"
    out = set()
    for e in plan["evidence"]["record"]["body"]["plan"]["entries"]:
        rel = e["file"]["path"]
        rel = rel[len(base):] if rel.startswith(base) else os.path.basename(rel)
        out.update((rel, rg) for rg in e["row_groups"])
    return out


def asof_us(rfc):
    import datetime
    d = datetime.datetime.strptime(rfc, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (d - epoch) // datetime.timedelta(microseconds=1)


def shape_rows(bin_, shape, work, small):
    out = os.path.join(work, shape)
    shutil.rmtree(out, ignore_errors=True)
    gen = ["bench", "tombstone", "--shape", shape, "--out", out]
    if small:
        gen += ["--files", "10", "--rows-per-row-group", "2000", "--keys", "20000",
                "--runs", "10,1000,10000"]
    run(bin_, gen)
    data = os.path.join(out, "data")
    w = json.load(open(os.path.join(out, "workload", "tombstone.json")))
    truth = json.load(open(os.path.join(out, "workload", "truth.json")))
    key, tomb = w["key"], w["tombstone"]
    tomb_col, _, tomb_value = tomb.partition("=")
    tomb_value = tomb_value or True
    plain_spec = f"temporal:{key}@ts"
    spec = f"{plain_spec},tombstone={tomb}"

    # Before: the temporal key alone; `why` prices the tombstone.
    run(bin_, ["index", data, "--key", plain_spec])
    first = truth["runs"][1]
    kf = os.path.join(out, "workload", f"{first['id']}.parquet")
    why = js(bin_, ["why", data, "--from", kf, "--latest-by", "ts", "--tombstone", tomb])
    cand = next(c for c in why["candidates"] if (c["spec"].get("tombstone") or {}))
    column = (cand.get("kind_column") or {}).get("value", "")
    lints = sorted({lint["rule"] for lint in why["profile"]["lints"]})
    recommended = why["recommended"]["spec"] if why.get("recommended") else None

    # After: the tombstone sidecar beside it.
    run(bin_, ["index", data, "--key", spec])
    manifest = json.load(open(os.path.join(data, ".bellwether", "manifest.json")))
    side_bytes = sum(s.get("bytes", 0) for s in manifest["segments"] if s.get("kind") == "tombstone")
    key_bytes = sum(s.get("bytes", 0) for s in manifest["segments"]
                    if not s.get("kind") and s["key_spec"] == plain_spec)
    rows = decode(data, key, tomb_col, tomb_value)
    at = asof_us(w["asof"])
    out_rows = []
    for r in truth["runs"]:
        kf = os.path.join(out, "workload", f"{r['id']}.parquet")
        keys = pq.read_table(kf).column(0).to_pylist()
        for lookup, t_at, args in [("latest", None, ["latest", data, "--keys", kf]),
                                   ("asof", at, ["asof", data, "--keys", kf, "--at", w["asof"]])]:
            plain = js(bin_, args)
            live = js(bin_, [*args, "--live"])
            newest_rg, live_rg, with_version, retracted = oracle(rows, keys, t_at)
            tr = r[lookup]
            assert (with_version, retracted) == (tr["keys_with_version"],
                                                 tr["newest_retracted"] + tr["newest_null"]), (shape, r["id"], lookup)
            assert (len(newest_rg), len(live_rg)) == (tr["newest_row_groups"], tr["live_row_groups"])
            exact = plan_row_groups(plain, data) == newest_rg and plan_row_groups(live, data) == live_rg
            out_rows.append({
                "keys": len(keys), "lookup": lookup, "with_version": with_version,
                "retracted": retracted,
                "plain_rg": plain["planned"]["row_groups"], "plain_bytes": plain["planned"]["bytes"],
                "live_rg": live["planned"]["row_groups"], "live_bytes": live["planned"]["bytes"],
                "pruned_by": live["live"].get("pruned_by") or "(none)",
                "keys_retracted": live["live"]["keys_retracted"],
                "oracle": (len(newest_rg), len(live_rg)), "exact": exact,
                "total": plain["row_groups_total"],
            })
            if not exact:
                print(f"<!-- MISMATCH {shape} {r['id']} {lookup} -->")
    return {
        "shape": shape, "summary": truth["summary"], "spec": spec, "filter": w["filter"],
        "asof": w["asof"], "why_column": column, "why_lints": lints, "why_keys": first["id"],
        "recommended": recommended, "side_bytes": side_bytes, "key_bytes": key_bytes,
        "rows": out_rows,
    }


def table(res):
    s = res["summary"]
    print(f"### {res['shape'].capitalize()}: {s['rows']:,} rows in {s['row_groups']:,} row groups, "
          f"{s['retractions']:,} retractions, {s['keys_with_version']:,} keys; `{res['spec']}` "
          f"(filter without it: `{res['filter']}`; asof at {res['asof']})\n")
    print(f"Over the corpus: {s['newest_retracted']:,} keys ({100 * s['newest_retracted'] / s['keys_with_version']:.1f}%) "
          f"have a retraction as their newest row, {s['only_retractions']:,} hold only retractions, "
          f"{s['resurrected']:,} were retracted and written again; {s['null_tombstones']:,} rows have a null tombstone column. "
          f"`why --latest-by ts --tombstone` on the {res['why_keys']} run, before the sidecar: "
          f"`tombstone` column {res['why_column']!r}, lints {', '.join(res['why_lints']) or '(none)'}. "
          f"Index bytes: {res['key_bytes']:,} for the temporal key, {res['side_bytes']:,} for the tombstone sidecar.\n")
    print("| keys | lookup | keys with a version | newest not live | asof + filter: row groups | asof + filter: bytes | "
          "--live: row groups | --live: bytes | --live: pruned by | --live: keys retracted | "
          "oracle row groups (newest / live newest) | plans = oracle |")
    print("|---:|---|---:|---:|---:|---:|---:|---:|---|---:|---:|---|")
    for r in res["rows"]:
        print(f"| {r['keys']:,} | {r['lookup']} | {r['with_version']:,} | {r['retracted']:,} | "
              f"{r['plain_rg']:,} of {r['total']:,} | {r['plain_bytes']:,} | {r['live_rg']:,} | {r['live_bytes']:,} | "
              f"`{r['pruned_by']}` | {r['keys_retracted']:,} | {r['oracle'][0]:,} / {r['oracle'][1]:,} | "
              f"{'yes' if r['exact'] else 'NO'} |")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work")
    ap.add_argument("--label", default="debug build")
    ap.add_argument("--small", action="store_true")
    a = ap.parse_args()
    work = os.path.realpath(a.work or tempfile.mkdtemp(prefix="bw-tombstone-"))
    os.makedirs(work, exist_ok=True)
    print("## Tombstones: `asof --live` against `asof` plus the filter\n")
    print(f"Machine: {machine()}. Build: {a.label}, commit {commit()}. "
          f"Row groups and bytes are exact; nothing here is timed.\n")
    for shape in SHAPES:
        table(shape_rows(a.bin, shape, work, a.small))


if __name__ == "__main__":
    main()
