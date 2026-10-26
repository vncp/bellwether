#!/usr/bin/env python3
"""Measured funnels for agg keys (B7) on both corpus shapes.

The operator is the grouped aggregate over a key set,
`sum(value), count(value) ... where key in (...) group by key`, and the
per-key totals a reconciliation compares. Two corpora:

- the ledger (`bellwether bench agg`): `account`, `version`, `amount:
  Decimal128(18, 6)` with 2% null amounts, 10^7 rows in 1,000 row groups
  over skewed accounts, key `account`, agg key
  `agg:account@amount{sum,count,min,max}` beside `exact:account,counts`;
- the time series (`bellwether bench market`, small rung, hive-partitioned
  by `day`): per-symbol daily volume, key `(symbol, day)`, agg key
  `agg:symbol,day@size{sum,count,min,max}` beside
  `exact:symbol,day,counts`. The corpus has fewer (symbol, day) pairs than
  the larger key sets, so those add absent symbols, answered as zero rows.

Key sets of 10, 10^3 and 10^5 distinct keys. For each (corpus, key set) the
per-key table reports, from the CLI's --json output (exact sums of
statistics, never estimates):

  live              live row groups in the index
  rows              rows of the keys (the aggregate's, from the counts)
  without agg       what reading the keys' rows costs: the row groups and
                    compressed bytes a locate of the keys plans through the
                    counted key, the bytes of only the key and value columns
                    over them (locate --columns), and the rows decoded
                    (every row of those row groups)
  with agg          row groups and bytes `aggregate` still reads (its
                    residual), and data files opened: 0 by construction when
                    the residual is empty (the aggregate reads the segments
                    only; crates/bellwether/tests/agg.rs)
  oracle            keys whose rows, null values, count and sum `aggregate`
                    returned equal pyarrow's over the decoded files, of the
                    keys asked (the ledger's run totals are also checked
                    against the generator's truth.json)

The reconcile table compares two copies of the ledger by `account` with
the exact total of `amount`, every other column ignored: both sides with
agg keys, both without (unindexed), and each again with one amount on the
right moved by 0.000001 in one file. It reports each side's path and the
rows it read, and whether the reconcile passed. Copies hard-link the data
files, so they cost no space.

Usage: scripts/agg-funnels.py --bin PATH/TO/bellwether [--work DIR]
       [--label "debug build"] [--small] [--reuse]

--small writes a 10^5-row ledger instead of 10^7, for a quick check;
--reuse keeps a ledger already generated and indexed under --work. Needs
python3 with pyarrow. Prints markdown; the label line names the machine and
the build (ADR 019). No timing is measured.
"""

import argparse
import datetime
import decimal
import json
import os
import platform
import random
import shutil
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

SIZES = [10, 1_000, 100_000]
LEDGER_AGG = "agg:account@amount{sum,count,min,max}"
MARKET_AGG = "agg:symbol,day@size{sum,count,min,max}"


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


def dataset(data, partitioning):
    return ds.dataset(data, format="parquet", partitioning=partitioning)


def oracle(data, partitioning, key_cols, value, keys):
    """Per key tuple (as displayed): [rows, nulls, exact sum of the non-null
    values] (Decimal for decimals, int otherwise), and the row groups holding
    a key and their rows (what a read of them decodes), from the decoded
    files one row group at a time."""
    wanted = set(keys)
    out = {}
    holding = decoded = 0
    dset = dataset(data, partitioning)
    for frag in dset.get_fragments():
        for rg in frag.split_by_row_group():
            t = rg.to_table(schema=dset.schema, columns=[*key_cols, value])
            cols = [t.column(i).to_pylist() for i in range(len(key_cols))]
            vals = t.column(len(key_cols)).to_pylist()
            hit = False
            for i, v in enumerate(vals):
                k = tuple(str(c[i]) for c in cols)
                if k not in wanted:
                    continue
                hit = True
                acc = out.setdefault(k, [0, 0, 0])
                acc[0] += 1
                if v is None:
                    acc[1] += 1
                else:
                    acc[2] += v
            if hit:
                holding += 1
                decoded += t.num_rows
    return out, holding, decoded


def check_groups(agg, want, scale):
    """Keys whose aggregate equals the oracle's: rows, nulls, count, sum."""
    good = 0
    for g in agg["groups"]:
        k = tuple(g["key"])
        rows, nulls, s = want.get(k, [0, 0, 0])
        sum_v = (g.get("sum") or {}).get("value")
        if scale:
            want_sum = None if rows == nulls else f"{decimal.Decimal(s).quantize(decimal.Decimal(1).scaleb(-scale))}"
        else:
            want_sum = None if rows == nulls else str(s)
        if (g["rows"], g["nulls"], g["count"], g["residual_rows"]) == (rows, nulls, rows - nulls, 0) \
                and (rows == 0 or sum_v == want_sum):
            good += 1
        else:
            print(f"<!-- mismatch {k}: got rows {g['rows']} nulls {g['nulls']} sum {sum_v}, "
                  f"want {rows} {nulls} {want_sum} -->")
    return good


def per_key_rows(bin_, name, data, extra, key_cols, value, sets, work, scale, partitioning, types):
    rows = []
    for n, keys in sets.items():
        kf = os.path.join(work, f"keys-{name}-{n}.parquet")
        if not os.path.exists(kf):
            cols = list(zip(*keys)) if keys else [[] for _ in key_cols]
            pq.write_table(pa.table({c: pa.array(list(v), type=t)
                                     for c, v, t in zip(key_cols, cols, types)}), kf)
        base = [data, "--keys", kf, *extra]
        agg = js(bin_, ["aggregate", *base, "--value", value, "--fn", "sum,count,min,max"])
        loc = js(bin_, ["locate", *base])
        proj = js(bin_, ["locate", *base, "--columns", ",".join([*key_cols, value])])
        want, holding, decoded = oracle(data, partitioning, key_cols, value,
                                        [tuple(str(x) for x in k) for k in keys])
        assert holding == loc["planned"]["row_groups"], (name, n, holding, loc["planned"])
        good = check_groups(agg, want, scale)
        rows.append({
            "keys": n,
            "present": sum(1 for k in want.values() if k[0]),
            "live": agg["row_groups_total"],
            "rows": agg["rows"],
            "oracle_rows": sum(v[0] for v in want.values()),
            "loc_rg": loc["planned"]["row_groups"],
            "loc_bytes": loc["planned"]["bytes"],
            "proj_bytes": proj["projected_bytes"],
            "decoded": decoded,
            "res_rg": agg["residual_row_groups"],
            "res_bytes": agg["residual_bytes"],
            "complete": agg["complete"],
            "good": good,
            "asked": agg["keys"],
            "sum_total": sum((v[2] for v in want.values()), 0),
            "nulls": sum(v[1] for v in want.values()),
        })
        assert agg["rows"] == rows[-1]["oracle_rows"], (name, n)
    return rows


def per_key_table(title, rows):
    print(f"### {title}\n")
    print("| keys | keys present | live row groups | rows of the keys (aggregate / oracle) | "
          "without agg: row groups | without agg: bytes | without agg: key+value column bytes | "
          "without agg: rows decoded | with agg: row groups read | with agg: bytes read | "
          "with agg: data files opened | keys equal to the oracle |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|")
    for r in rows:
        opened = "0 by construction" if r["complete"] and r["res_rg"] == 0 else "residual files"
        print(f"| {r['keys']:,} | {r['present']:,} | {r['live']:,} | {r['rows']:,} / {r['oracle_rows']:,} | "
              f"{r['loc_rg']:,} | {r['loc_bytes']:,} | {r['proj_bytes']:,} | {r['decoded']:,} | "
              f"{r['res_rg']:,} | {r['res_bytes']:,} | {opened} | {r['good']:,} of {r['asked']:,} |")
    print()


def link_copy(src, dst):
    """Hard-links every data file of `src` into `dst` (no index)."""
    shutil.rmtree(dst, ignore_errors=True)

    def ignore(_dir, names):
        return [n for n in names if n.startswith(".")]

    shutil.copytree(src, dst, copy_function=os.link, ignore=ignore)


def plant(data, rel):
    """Rewrites one file with its first non-null amount moved by one unit
    (0.000001), row groups kept. Returns the row moved."""
    path = os.path.join(data, rel)
    f = pq.ParquetFile(path)
    rgs = [f.read_row_group(i) for i in range(f.metadata.num_row_groups)]
    size = rgs[0].num_rows
    t = pa.concat_tables(rgs)
    amounts = t.column("amount").to_pylist()
    i = next(j for j, v in enumerate(amounts) if v is not None)
    amounts[i] += decimal.Decimal("0.000001")
    t = t.set_column(t.schema.get_field_index("amount"), t.schema.field("amount"),
                     pa.array(amounts, type=t.schema.field("amount").type))
    os.unlink(path)  # a hard link: never write through it
    pq.write_table(t, path, row_group_size=size, compression="snappy")
    return i


def reconcile_rows(bin_, ledger, work):
    data = os.path.join(ledger, "data")
    right = os.path.join(work, "ledger-right")
    plain_l = os.path.join(work, "ledger-plain-left")
    plain_r = os.path.join(work, "ledger-plain-right")
    bad = os.path.join(work, "ledger-right-planted")
    bad_plain = os.path.join(work, "ledger-plain-right-planted")
    for d in (right, plain_l, plain_r, bad, bad_plain):
        link_copy(data, d)
    files = sorted(f for f in os.listdir(data) if f.endswith(".parquet"))
    for d in (bad, bad_plain):
        plant(d, files[len(files) // 2])
    for d in (right, bad):
        run(bin_, ["index", d, "--key", "exact:account,counts", "--key", LEDGER_AGG])
    # One total per account: about 6 x 10^5 groups on the 10^7-row ledger,
    # more than the default 256 MiB state budget holds, so it is raised.
    args = ["--key", "account", "--ignore", "version,amount", "--totals", "amount",
            "--group-by", "account", "--memory-budget", str(4 << 30)]
    out = []
    for label, l, r in [
        ("agg keys on both sides", data, right),
        ("no index on either side", plain_l, plain_r),
        ("agg keys, one amount off by 0.000001", data, bad),
        ("no index, one amount off by 0.000001", plain_l, bad_plain),
    ]:
        res = subprocess.run([bin_, "reconcile", l, r, *args, "--json"], capture_output=True, text=True)
        if res.returncode not in (0, 1):
            sys.exit(f"reconcile {label} failed ({res.returncode}): {res.stderr.strip()}")
        j = json.loads(res.stdout)
        o = j["outcome"]
        out.append({
            "label": label,
            "paths": [s["path"]["kind"] for s in o["sides"]],
            "rows": [s["rows"] for s in o["sides"]],
            "read": o["memory"]["rows_read"],
            "pass": o["pass"],
            "totals_differ": o["counts"]["totals_differ"],
            "agg": [a["side"] for a in o.get("agg", [])],
            "lints": [lint["rule"] for lint in j.get("lints", [])],
            "differ": [(t["group"], t["left"], t["right"]) for t in o["totals"]
                       if t["left"] != t["right"]],
        })
    return out


def exact(t):
    return decimal.Decimal(int(t["unscaled"]["value"])).scaleb(-t["scale"])


def reconcile_table(rows):
    print("### Reconcile two copies of the ledger: per-key rows and the exact total of `amount`\n")
    print("| sides | left path | right path | rows (left / right) | rows read (left / right) | passed | "
          "per-account totals differing | certificate `agg` | lints |")
    print("|---|---|---|---:|---:|---|---|---|---|")
    for r in rows:
        differ = "; ".join(
            f"account {dict(g['values'])['account']}: {exact(left)} vs {exact(right)}"
            for g, left, right in r["differ"]) or "0"
        print(f"| {r['label']} | {r['paths'][0]} | {r['paths'][1]} | {r['rows'][0]:,} / {r['rows'][1]:,} | "
              f"{r['read'][0]:,} / {r['read'][1]:,} | {'yes' if r['pass'] else 'no'} | {differ} | "
              f"{', '.join(r['agg']) or '(none)'} | {', '.join(sorted(set(r['lints']))) or '(none)'} |")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--work")
    ap.add_argument("--label", default="debug build")
    ap.add_argument("--small", action="store_true")
    ap.add_argument("--reuse", action="store_true",
                    help="keep a ledger corpus and index already under --work")
    a = ap.parse_args()
    work = os.path.realpath(a.work or tempfile.mkdtemp(prefix="bw-agg-"))
    os.makedirs(work, exist_ok=True)
    rng = random.Random(7)

    print("## Agg keys: per-key sums over a key set, and reconcile from the index\n")
    print(f"Machine: {machine()}. Build: {a.label}, commit {commit()}. "
          f"Row groups, bytes, rows and sums are exact; nothing here is timed.\n")

    # Ledger.
    ledger = os.path.join(work, "ledger")
    data = os.path.join(ledger, "data")
    if not (a.reuse and os.path.exists(os.path.join(ledger, "workload", "truth.json"))):
        shutil.rmtree(ledger, ignore_errors=True)
        gen = ["bench", "agg", "--out", ledger]
        if a.small:
            gen += ["--files", "10", "--row-groups-per-file", "10", "--rows-per-row-group", "1000",
                    "--accounts", "200000", "--keys", "10,1000,10000"]
        run(a.bin, gen)
        run(a.bin, ["index", data, "--key", "exact:account,counts", "--key", LEDGER_AGG])
    truth = json.load(open(os.path.join(ledger, "workload", "truth.json")))
    sets = {}
    for r in truth["runs"]:
        n = int(r["id"].split("-")[1])
        keys = pq.read_table(os.path.join(ledger, "workload", f"{r['id']}.parquet")).column(0).to_pylist()
        sets[n] = [(k,) for k in keys]
    rows = per_key_rows(a.bin, "ledger", data, [], ["account"], "amount", sets, work, 6, None,
                        [pa.int64()])
    for r, t in zip(rows, truth["runs"]):
        assert r["oracle_rows"] == t["rows"] and r["nulls"] == t["null_amounts"], (r, t)
        assert f"{decimal.Decimal(r['sum_total']).quantize(decimal.Decimal('0.000001'))}" == t["sum"], (r, t)
    s = truth["summary"]
    per_key_table(f"Ledger corpus: {s['rows']:,} rows in {s['row_groups']:,} row groups, "
                  f"{s['null_amounts']:,} null amounts, {s['head_rows']:,} rows on the busiest account; "
                  f"`exact:account,counts` + `{LEDGER_AGG}`", rows)
    reconcile_table(reconcile_rows(a.bin, ledger, work))

    # Market: the small rung, per-symbol daily volume.
    market = os.path.join(work, "market")
    shutil.rmtree(market, ignore_errors=True)
    run(a.bin, ["bench", "market", "--out", market, "--rung", "small"])
    mdata = os.path.join(market, "data")
    extra = ["--partitioning", "hive"]
    run(a.bin, ["index", mdata, *extra, "--key", "exact:symbol,day,counts", "--key", MARKET_AGG])
    present = set()
    for b in dataset(mdata, "hive").to_batches(columns=["symbol", "day"]):
        # bellwether types the `day=` path value as a date (Date32).
        present.update(zip(b.column(0).to_pylist(),
                           (datetime.date.fromisoformat(str(d)) for d in b.column(1).to_pylist())))
    present = sorted(present)
    rng.shuffle(present)
    days = sorted({d for _, d in present})
    msets = {}
    for n in SIZES:
        take = present[:n]
        extra_keys = [(f"X{i:06d}", days[i % len(days)]) for i in range(n - len(take))]
        msets[n] = take + extra_keys
    mrows = per_key_rows(a.bin, "market", mdata, extra, ["symbol", "day"], "size", msets, work, 0,
                         "hive", [pa.string(), pa.date32()])
    per_key_table(f"Market corpus (small rung): {len(present):,} (symbol, day) pairs present, "
                  f"`exact:symbol,day,counts` + `{MARKET_AGG}`; larger key sets add absent symbols",
                  mrows)


if __name__ == "__main__":
    main()
