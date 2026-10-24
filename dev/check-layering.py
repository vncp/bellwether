#!/usr/bin/env python3
"""Enforce the crate layering in docs/architecture.md mechanically.

Dependencies point one way. Each workspace crate may depend only on crates in
the set listed here. A new edge is an architectural decision: add it here with
a line in the PR saying why, or the check fails.
"""
import json
import subprocess
import sys

ALLOWED = {
    "bellwether-types": set(),
    "bellwether-core": {"bellwether-types"},
    "bellwether-source": {"bellwether-types"},
    "bellwether-store": {"bellwether-types"},
    "bellwether-ledger": {"bellwether-types"},
    "bellwether-bench": {"bellwether-types", "bellwether-source"},
    "bellwether-build": {"bellwether-types", "bellwether-core", "bellwether-source", "bellwether-store"},
    "bellwether-profile": {"bellwether-types", "bellwether-core", "bellwether-source", "bellwether-store", "bellwether-ledger"},
    "bellwether-replay": {"bellwether-types", "bellwether-core", "bellwether-source", "bellwether-store", "bellwether-ledger", "bellwether-build"},
    "bellwether-verify": {"bellwether-types", "bellwether-core", "bellwether-source", "bellwether-store", "bellwether-ledger", "bellwether-build"},
    "bellwether-evolve": {"bellwether-types", "bellwether-core", "bellwether-source", "bellwether-store", "bellwether-ledger", "bellwether-build", "bellwether-profile", "bellwether-replay", "bellwether-verify"},
    "bellwether": {"bellwether-types", "bellwether-core", "bellwether-source", "bellwether-store", "bellwether-ledger", "bellwether-build", "bellwether-profile", "bellwether-replay", "bellwether-verify", "bellwether-evolve"},  # the facade: the one code path the CLI and MCP share (ADR 010)
    "bellwether-cli": {"bellwether", "bellwether-types", "bellwether-bench", "bellwether-mcp"},  # formatting and argument parsing only; `bench` and `mcp` subcommands delegate
    "bellwether-mcp": {"bellwether", "bellwether-types"},  # tool envelopes over the facade; no engine logic
}


def main() -> int:
    meta = json.loads(
        subprocess.check_output(["cargo", "metadata", "--format-version", "1", "--no-deps"])
    )
    workspace = {p["name"] for p in meta["packages"]}
    failures = []
    for pkg in meta["packages"]:
        name = pkg["name"]
        if name not in ALLOWED:
            failures.append(f"{name}: not listed in dev/check-layering.py ALLOWED; add it with its permitted edges")
            continue
        allowed = ALLOWED[name]
        if allowed is None:
            continue
        for dep in pkg["dependencies"]:
            if dep["name"] in workspace and dep["name"] not in allowed:
                failures.append(f"{name} -> {dep['name']}: edge not permitted by the layering")
    for f in failures:
        print("layering:", f, file=sys.stderr)
    if failures:
        return 1
    print(f"layering: {len(workspace)} workspace crates, all edges permitted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
