"""Builds `fixtures/format-v4/` (record version 4, B8 tombstones): an index
of `live/` (written by `live.py`), its manifest history, and a ledger
holding a live chain, with a given `bellwether` binary; pins what that
binary answered in `expected.json`, in the layout of the other fixtures
(`build_fixture.py`).

Run from the repository root:

    python3 fixtures/format-corpus/build_live_fixture.py --bin <path to bellwether> \
        --out fixtures/format-v4 --root /tmp/bellwether-format-v4

The steps, every one under the fixed clock 2026-10-25T00:00:00Z:

1. copy `live/data/` to `<root>/data` and set every file's mtime to the
   fixed clock;
2. `index` with `exact:id` and `temporal2:id@valid,known,tombstone=status=CANCEL`
   (a bitemporal key with a value tombstone, its `tombstone` sidecar;
   sequence 1);
3. `why --from live/keys.parquet --latest-by ts --tombstone void` (profile
   Evidence, and a workload of live `latest` runs), `propose --key
   temporal:id@ts,tombstone=void` citing it (a Proposal whose key spec
   carries a tombstone), `fix --verify-only` (Replay, Certificate whose row
   filter ends in `live`), then `fix --certificate` (the apply tier: plan
   Evidence, Receipt; sequence 2);
4. every pinned lookup with `--json` (`latest`/`asof` with and without
   `--live`, temporal and bitemporal, and `locate`: plan Evidence whose
   residual ends in `live`), and `receipt show` (the explain chain text).

`expected.json` holds what `build_fixture.py` writes (no failing
certificate) plus `certificate`, the hash of the passing certificate
`fix --certificate` applied, and `corpus`, the corpus folder (`live`) a
restore copies.
"""

import argparse
import json
import os
import shutil
import subprocess

from build_fixture import CLOCK, HERE, copy_tree, h

LIVE = os.path.join(HERE, "live")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--root", required=True)
    a = ap.parse_args()

    shutil.rmtree(a.root, ignore_errors=True)
    os.makedirs(a.root)
    root = os.path.realpath(a.root)
    data = os.path.join(root, "data")
    copy_tree(os.path.join(LIVE, "data"), data)
    steps = []

    def run(*args, ok=(0,)):
        cmd = [a.bin, "--clock", CLOCK, *args]
        p = subprocess.run(cmd, capture_output=True, text=True, cwd=root)
        steps.append({"args": list(args), "exit": p.returncode})
        if p.returncode not in ok:
            raise SystemExit(f"{' '.join(cmd)} failed ({p.returncode}):\n{p.stderr}{p.stdout}")
        return p

    def last(p):
        return json.loads(p.stdout.splitlines()[-1])

    keys = os.path.join(LIVE, "keys.parquet")
    run("index", data, "--key", "exact:id", "--key", "temporal2:id@valid,known,tombstone=status=CANCEL")
    why = last(run("--json", "why", data, "--from", keys, "--latest-by", "ts", "--tombstone", "void"))
    workload = why["workload"]["hash"]
    evidence = why["evidence"]["hash"]
    proposal = h(
        last(
            run(
                "--json", "propose", data, "--key", "temporal:id@ts,tombstone=void", "--by", "fixture",
                "--evidence", evidence,
            )
        )["proposal"]
    )
    verified = last(run("--json", "fix", data, "--proposal", proposal, "--workload", workload, "--verify-only"))
    if verified["outcome"].get("kind") != "verified":
        raise SystemExit(f"fix --verify-only wrote no passing certificate: {json.dumps(verified['outcome'])}")
    certificate = h(verified["certificate"])
    fix = last(run("--json", "fix", data, "--certificate", certificate))
    if fix["outcome"].get("kind") != "applied":
        raise SystemExit(f"fix --certificate wrote no receipt: {json.dumps(fix['outcome'])}")
    receipt = h(fix["outcome"]["receipt"])

    at = "2026-01-01T00:16:30Z"
    lookups = [
        ["latest", data, "--keys", keys, "--live"],
        ["latest", data, "--keys", keys],
        ["asof", data, "--keys", keys, "--at", at, "--live"],
        ["asof", data, "--keys", keys, "--at", at],
        ["asof", data, "--keys", keys, "--valid", "2026-01-02T00:00:00Z", "--known", "2026-01-01T03:00:00Z", "--live"],
        ["latest", data, "--keys", keys, "--known", "2026-01-01T02:00:00Z", "--live"],
        ["locate", data, "--keys", keys],
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
    # Keep the recorded workload and its key sets (cache objects named by
    # their hashes), so a reader can be run on the live workload itself.
    cache = os.path.join(data, ".bellwether", "cache")
    with open(os.path.join(cache, f"{workload}.bin")) as f:
        kept = [workload] + [r["key_set"] for r in json.load(f)["runs"]]
    os.makedirs(os.path.join(out, "index", "cache"))
    for k in kept:
        for ext in (".bin", ".meta.json"):
            shutil.copyfile(os.path.join(cache, k + ext), os.path.join(out, "index", "cache", k + ext))

    ledger = sorted(
        f[: -len(".rec")] for f in os.listdir(os.path.join(out, "index", "ledger")) if f.endswith(".rec")
    )
    expected = {
        "root": root,
        "clock": CLOCK,
        "corpus": "live",
        "steps": [{"args": [x.replace(HERE, "<corpus>") for x in s["args"]], "exit": s["exit"]} for s in steps],
        "workload": workload,
        "proposal": proposal,
        "certificate": certificate,
        "receipt": receipt,
        "lookups": pinned,
        "receipt_text": explain,
        "records": ledger,
    }
    with open(os.path.join(out, "expected.json"), "w") as f:
        json.dump(expected, f, indent=1, sort_keys=True)
        f.write("\n")


if __name__ == "__main__":
    main()
