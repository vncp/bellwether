# bellwether-cli

The `bellwether` command: argument parsing and formatting over the facade (ADR 010). Every command is one call into `bellwether::Engine`; `--json` prints that call's typed result, serialized, as one line, the same value the MCP server returns. `bellwether --help` lists the commands and `bellwether <command> --help` their flags.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | success; for `ci`, every changed step passed, the gate is `advise`, or no step changed |
| 1 | a check failed: a certificate failed, a reconcile found mismatches, `receipt show --require-signature` found no trusted signature, an index's join key could not be rebuilt, a bench SLO failed, or `ci` found a changed step without a receipt that stands under `gate = "require"` |
| 2 | usage: a flag, an argument, or for `ci` a malformed configuration (a path outside the repository, an unknown field, a step name that is not a plain file name, a workload that is not a 64-hex hash) |
| 3 | refused at the trust boundary |
| 4 | engine error; for `ci`, the ledger could not be read |

An internal panic prints the issue tracker and exits 4.

## `ci`: gate a pull request on receipts (WP17)

```bash
bellwether ci --base <sha> --head <sha> [--config .bellwether/ci.toml] [--changed-files <file>] \
  [--pr-body <file>] [--comment-out <file>] [--repo <dir>] [--json]
```

- The configuration is read from the **base** commit only (`git cat-file <base>:<config>`), so a pull request cannot change its own gate. The trust file it names is read from the base too.
- The changed steps are those with a path glob (`*` and `?` within a path part, `**` across parts) matching a path of `git diff --name-only <base>...<head>` (renames list both names), or of `--changed-files`. Unchanged steps are not evaluated; no changed step is "nothing to check" (exit 0).
- Each changed step's receipt hash is taken from `.bellwether/receipts/<step>` at the head (exactly 64 lowercase hex characters and at most one newline) or a whole line `Bellwether-Receipt(<step>): <hash>` of `--pr-body`. Anything else is "no receipt given"; two different hashes are refused, never chosen between.
- Each receipt goes through `Engine::receipt_gate` (the facade README lists its six checks). Without a receipt, under `gate = "advise"` with `[ci] why = true`, `why` runs over the step's workload and the comment shows its funnel and the command to produce the receipt.
- `--comment-out` writes the pull request comment, its first line the marker `<!-- bellwether-ci -->` that the Action finds and updates it by. `--json` prints the facade's `GateReport`.
- Git is asked for object contents and path names only; nothing from the head is run.

The configuration:

```toml
ledger = "lake/positions/.bellwether"  # the index location whose ledger holds the receipts (a local path is relative to the repository)
# root = "lake/positions"              # its data root; inferred for a local ledger named .bellwether
gate = "require"                       # require | advise
require_signature = true
trusted_keys = ".bellwether/trusted-keys.toml"   # in the repository, read from the base

[ci]
why = false                            # under advise: run `why` for a step without a receipt (writes to the ledger)

[[step]]
name = "daily_positions"               # also the receipt file's name
paths = ["pipelines/daily_positions.sql", "pipelines/positions/*.py"]
root = "."                             # the folder the receipt is about, relative to the data root
workload = "3f9c..."                   # the recorded workload hash the certificate must cover
```
