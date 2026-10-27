<!-- bellwether-ci -->
### ❌ 2 of 2 steps failed the receipt gate

| step | receipt | certificate | bytes before → after | manifest before → after | signature | verdict |
|---|---|---|---|---|---|---|
| `positions` | none | — | — | — | — | ❌ no receipt given (below: how to produce it) |
| `repo_counts` | `<h1>` | — | — | — | — | ❌ wrong_kind: <h1> is a certificate, not a receipt; give the receipt hash `bellwether fix` printed for repo_counts (not a certificate or a proposal) |

#### `positions`: no receipt given

Produce the receipt:

```sh
bellwether why <tmp>/repo/lake/events --workload <h2>
bellwether fix <tmp>/repo/lake/events --proposal <proposal why printed> --workload <h2>
# then commit the receipt hash to .bellwether/receipts/positions or add the line `Bellwether-Receipt(positions): <receipt>` to the pull request body
```

<sub>bellwether <version> · gate `require` · ledger `<tmp>/repo/lake/events/.bellwether` · base `<h3>` · head `<h4>` · `bellwether --clock 2026-10-27T12:00:00.000Z ci --repo <tmp>/repo --base <h3> --head <h4> --comment-out <tmp>/comment.md --pr-body <tmp>/body.md`</sub>
