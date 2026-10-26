"""Builds one persisted-format fixture (an index, its manifest history, and a
ledger) over this corpus with a given `bellwether` binary, and pins what that
binary answered in `expected.json`.

Run from the repository root:

    python3 fixtures/format-corpus/build_fixture.py --bin <path to bellwether> \
        --out fixtures/format-v1 --root /tmp/bellwether-format-v1

`--counts` declares the counted forms of the exact and temporal specs (a
storage version 2 binary only). `--m5` (storage version 2 at M5, implies
`--counts`) declares the folded forms instead: `exact:id@ts,counts` (per-key
counts and time bounds in one value record), `bloom:id@0.01@ts` (a bloom
with its bounds segment), `temporal2:id@valid,known` as a bitemporal key
spec; proposes `exact:symbol@ts,counts`; indexes `text/` (a CSV file,
so the manifest holds a `source.dialect`) beside the Parquet folder; and
indexes `nested/` (list, struct, map and dictionary columns, written by
`nested.py`, so the manifest holds the `{"arrow": "<display>"}` spelling). The
steps, every one under the fixed clock 2026-10-25T00:00:00Z:

1. copy `data/` to `<root>/data` and set every file's mtime to the fixed
   clock, so a fingerprint's etag is the same wherever the fixture is
   restored;
2. `index --partitioning hive` with `exact:id@ts` (per-key time bounds),
   `bloom:id@0.01`, `range:ts`, `temporal:id@ts`, `temporal2:id@valid,known`
   (sequence 1);
3. add `extra/day=2026-01-03/part-0.parquet` and `index` again (sequence 2);
4. `why --from keys-symbol.parquet` (profile Evidence, the recorded
   workload), `propose --key exact:symbol` citing it, `fix` (Replay, plan
   Evidence, Certificate, Receipt; sequence 3);
5. `reconcile` against `right/` (one amount differs): a failing Certificate;
6. every pinned lookup with `--json`, and `receipt` (the explain chain text);
   with `--counts`, also `count` and `locate --columns`; with `--m5`, also
   `locate` and `count` over `<root>/text`, whose index is copied to
   `index-text/`, and `locate` over `<root>/nested` (indexed with
   `exact:id`), whose index is copied to `index-nested/`.

`expected.json` holds the root the fixture was built at, the clock, every
lookup's argument list and its output lines, the receipt text, and the hash
of every ledger record. A test restores the fixture under another root by
replacing the recorded root in the manifests and in the pinned outputs.
`text/trades.csv` is written by hand, not by `generate.py`.
"""

import argparse
import datetime
import json
import os
import shutil
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
CLOCK = "2026-10-25T00:00:00Z"
MTIME = datetime.datetime(2026, 10, 25, tzinfo=datetime.timezone.utc).timestamp()


def copy_tree(src, dst):
    for dirpath, _, files in os.walk(src):
        for f in files:
            s = os.path.join(dirpath, f)
            d = os.path.join(dst, os.path.relpath(s, src))
            os.makedirs(os.path.dirname(d), exist_ok=True)
            shutil.copyfile(s, d)
            os.utime(d, (MTIME, MTIME))


def h(x):
    """A hash from a result field: the hash itself, or an object holding it."""
    return x["hash"] if isinstance(x, dict) else x


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--root", required=True)
    ap.add_argument("--counts", action="store_true")
    ap.add_argument("--m5", action="store_true")
    a = ap.parse_args()

    root = os.path.realpath(a.root) if os.path.exists(a.root) else a.root
    shutil.rmtree(a.root, ignore_errors=True)
    os.makedirs(a.root)
    root = os.path.realpath(a.root)
    data = os.path.join(root, "data")
    right = os.path.join(root, "right")
    copy_tree(os.path.join(HERE, "data"), data)
    copy_tree(os.path.join(HERE, "right"), right)
    text = os.path.join(root, "text")
    nested = os.path.join(root, "nested")
    if a.m5:
        a.counts = True
        copy_tree(os.path.join(HERE, "text"), text)
        copy_tree(os.path.join(HERE, "nested"), nested)
    steps = []

    def run(*args, ok=(0,)):
        cmd = [a.bin, "--clock", CLOCK, *args]
        p = subprocess.run(cmd, capture_output=True, text=True, cwd=root)
        steps.append({"args": list(args), "exit": p.returncode})
        if p.returncode not in ok:
            raise SystemExit(f"{' '.join(cmd)} failed ({p.returncode}):\n{p.stderr}{p.stdout}")
        return p

    suffix = ",counts" if a.counts else ""
    if a.m5:
        keys = [
            "exact:id@ts,counts",
            "bloom:id@0.01@ts",
            "range:ts",
            "temporal:id@ts,counts",
            "temporal2:id@valid,known",
        ]
    else:
        keys = [
            "exact:id@ts",
            "bloom:id@0.01",
            "range:ts",
            f"temporal:id@ts{suffix}",
            "temporal2:id@valid,known",
        ]
    args = ["index", data, "--partitioning", "hive"]
    for k in keys:
        args += ["--key", k]
    run(*args)
    copy_tree(os.path.join(HERE, "extra"), data)
    run("index", data)
    if a.m5:
        run("--format", "csv", "index", text, "--key", "exact:id,counts")
        run("index", nested, "--key", "exact:id")

    why = json.loads(
        run("--json", "why", data, "--from", os.path.join(HERE, "keys-symbol.parquet")).stdout.splitlines()[-1]
    )
    workload = why["workload"]["hash"]
    evidence = why["evidence"]["hash"]
    proposal_key = "exact:symbol@ts,counts" if a.m5 else "exact:symbol,counts" if a.counts else "exact:symbol"
    prop = json.loads(
        run(
            "--json", "propose", data, "--key", proposal_key, "--by", "fixture",
            "--evidence", evidence,
        ).stdout.splitlines()[-1]
    )
    proposal = h(prop["proposal"])
    fix = json.loads(
        run("--json", "fix", data, "--proposal", proposal, "--workload", workload).stdout.splitlines()[-1]
    )
    if fix["outcome"].get("kind") != "applied":
        raise SystemExit(f"fix wrote no receipt: {json.dumps(fix['outcome'])}")
    receipt = h(fix["outcome"]["receipt"])
    rec = run(
        "--json", "--partitioning", "hive", "reconcile", data, right, "--key", "id,ts", "--totals", "amount",
        ok=(1,),
    )

    ids = os.path.join(HERE, "keys-id.parquet")
    symbols = os.path.join(HERE, "keys-symbol.parquet")
    lookups = [
        ["locate", data, "--keys", ids],
        ["locate", data, "--keys", ids, "--at-manifest", "1"],
        ["locate", data, "--keys", ids, "--at-manifest", "2"],
        ["locate", data, "--keys", symbols],
        ["locate", data, "--keys", ids, "--window", "2026-01-01T00:00:00Z..2026-01-01T06:00:00Z"],
        ["locate", data, "--keys", symbols, "--from", "2026-01-02T00:00:00Z", "--to", "2026-01-02T12:00:00Z"],
        ["latest", data, "--keys", ids],
        ["asof", data, "--keys", ids, "--at", "2026-01-02T00:00:00Z"],
        ["asof", data, "--keys", ids, "--valid", "2026-01-01T00:00:00Z", "--known", "2026-01-02T12:00:00Z"],
    ]
    if a.counts:
        # Storage version 2 only: a count from per-key counts, and a plan
        # with per-column bytes.
        lookups += [
            ["count", data, "--keys", ids],
            ["count", data, "--keys", symbols],
            ["locate", data, "--keys", ids, "--columns", "id,amount"],
        ]
    if a.m5:
        # M5: a CSV source's index (`source.dialect`).
        lookups += [
            ["--format", "csv", "locate", text, "--keys", ids],
            ["--format", "csv", "count", text, "--keys", ids],
            # M5: nested and dictionary columns (`{"arrow": …}`).
            ["locate", nested, "--keys", ids],
        ]
    pinned = []
    for args in lookups:
        out = run("--json", *args).stdout.splitlines()
        pinned.append({"args": [x.replace(HERE, "<corpus>") for x in args], "json": out})
    explain = run("receipt", "show", data, receipt).stdout

    out = os.path.abspath(a.out)
    shutil.rmtree(os.path.join(out, "index"), ignore_errors=True)
    shutil.copytree(
        os.path.join(data, ".bellwether"), os.path.join(out, "index"), ignore=shutil.ignore_patterns("*.lock#*")
    )
    shutil.rmtree(os.path.join(out, "index", "cache"), ignore_errors=True)
    if a.m5:
        shutil.rmtree(os.path.join(out, "index-text"), ignore_errors=True)
        shutil.copytree(
            os.path.join(text, ".bellwether"),
            os.path.join(out, "index-text"),
            ignore=shutil.ignore_patterns("*.lock#*"),
        )
        shutil.rmtree(os.path.join(out, "index-text", "cache"), ignore_errors=True)
        shutil.rmtree(os.path.join(out, "index-nested"), ignore_errors=True)
        shutil.copytree(
            os.path.join(nested, ".bellwether"),
            os.path.join(out, "index-nested"),
            ignore=shutil.ignore_patterns("*.lock#*"),
        )
        shutil.rmtree(os.path.join(out, "index-nested", "cache"), ignore_errors=True)
    ledger = sorted(
        f[: -len(".rec")] for f in os.listdir(os.path.join(out, "index", "ledger")) if f.endswith(".rec")
    )
    expected = {
        "root": root,
        "clock": CLOCK,
        "steps": [{"args": [x.replace(HERE, "<corpus>") for x in s["args"]], "exit": s["exit"]} for s in steps],
        "workload": workload,
        "proposal": proposal,
        "receipt": receipt,
        "failing_certificate": h(json.loads(rec.stdout.splitlines()[-1])["certificate"]),
        "lookups": pinned,
        "receipt_text": explain,
        "records": ledger,
    }
    with open(os.path.join(out, "expected.json"), "w") as f:
        json.dump(expected, f, indent=1, sort_keys=True)
        f.write("\n")


if __name__ == "__main__":
    main()
