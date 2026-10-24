# Threat model for the agent surface

**Claim:** a compromised, confused, or hallucinating agent can waste your time by proposing bad ideas, and can do nothing else.

| Surface | What it can do | Worst case |
|---|---|---|
| `observe.*` | read Evidence from cache; no network, no writes | the agent reads evidence |
| `propose.*` | write a `Proposal` with no score | the ledger holds a bad proposal that scores poorly and never gets a Certificate |
| `apply.*` | take a Certificate id only; the engine applies and then writes the Receipt | the agent asks to apply something unverified and is told no |

Rules that make the claim true:

- No tool accepts a file path to write, a shell command, or a query string to execute. Rewrites are data (`RewriteShape`: before, after, invariant), never code the engine runs.
- The engine never calls the model. The model calls the engine. The direction of dependency is the security property.
- `apply.*` resolves the Certificate and refuses unless `pass == true` and the certificate was computed against the current manifest version. Only after the apply succeeds does the engine write the Receipt, recording `manifest_before` (the version the certificate was computed against) and `manifest_after` (the version the apply produced).
- `Proposal` has no score field. Scoring cannot be injected because there is nowhere to put it.
- Every Evidence and Certificate carries provenance: inputs, age, creation time, and the manifest version when one exists (always for a Certificate; an Evidence about an unindexed folder has none yet). An agent cannot present stale numbers as fresh; the age is on the record.

Out of scope: an attacker with write access to the store. Content addressing makes tampering detectable but not preventable; signing is a later addition.
