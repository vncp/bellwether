# bellwether-ledger

Content-addressed, append-only ledger of Bellwether records (ADR 006). The ledger enforces the trust boundary (ADR 009) at write time. Nothing reaches *apply* unless a resolving, passing chain of records says why.

Owns **invariant 2** (a Receipt implies a passing Certificate) and **invariant 6** (Evidence and Certificates carry provenance).

## Layout

```
ledger/<blake3>.rec    canonical JSON of bellwether_types::Record
```

The name is `Record::hash()`, which is BLAKE3 over `serde_json::to_vec(&record)`, written as lowercase hex. The file holds exactly those bytes. `record_path` and `parse_record_path` are exact inverses: a name with uppercase hex is not a ledger record, and `list` skips it.

## API

| Call | What it does |
|---|---|
| `Ledger::new(store)` | Wraps anything that derefs to a `bellwether_types::Store` (`&S`, `Box<S>`, `Arc<dyn Store>`). The store is not exposed again; a `compile_fail` doctest pins that. |
| `put(&Record) -> Hash` | Checks the write-time rules and writes. Idempotent, because it is content-addressed. Refuses a `Receipt` (see below). |
| `put_receipt(&Receipt, current_manifest) -> Hash` | The only way a Receipt enters the ledger. Stage 4 is apply, then attest: `current_manifest` is the manifest sequence (ADR 020) current when the receipt is written, which is at or after the sequence the apply produced. |
| `get(&Hash) -> Record` | Verifies the hash, the version, and the kind, then parses. Returns `Error::NotFound` if the record is absent. |
| `get_raw(&Hash) -> Bytes` | Returns the verified bytes exactly as stored, so unknown fields are never lost. |
| `kind(&Hash) -> RecordKind` | Reads the header only. |
| `list(RecordKind) -> impl Iterator<Item = Result<Hash>>` | Lazy. Reads each record's header as the iterator advances. Skips records this build cannot read. |
| `list_unreadable() -> impl Iterator<Item = Result<Hash>>` | The records `list` skips: a newer record version or an unknown kind. |
| `explain(&Hash) -> Chain` | Walks a receipt's chain. `Chain::render()` (or `Display`) gives the terminal text. `Chain` is `Serialize` for `--json`. A bad link is flagged, never an error. |

`put` and `put_receipt` are the only write paths. There is no delete.

## Write-time rules

Every violation is a typed error. A rule violation is `Error::LedgerRule(msg)`, and an unsupported version is `Error::UnsupportedVersion`. Nothing is written when a rule fails.

**All records**

Durations (`oldest_input_age`) are encoded as integer nanoseconds (contract v2), so every value up to about 584 years round-trips exactly and the write-back check never refuses an age for precision. A duration beyond u64 nanoseconds cannot be encoded and is refused.

**Evidence** (invariant 6)

4. `provenance.inputs` must not be empty.
5. `provenance.manifest_version` is optional, because a profile of an unindexed folder has no manifest yet.

**Proposal**

6. Every `evidence` hash that resolves must resolve to Evidence. An unresolvable hash is allowed and shows as `MISSING` in `explain`. There is no score; the agent may say anything else (ADR 009).

   This kind check runs **at write time only**. A cited hash that was absent when the proposal was written is not re-checked when a record later appears under it. A receipt citing the proposal does not re-check the proposal's evidence either. If such a link turns out to be another kind, `explain` flags it as `WRONG KIND`.

**Replay** (contract v2.1)

7. `proposal` must resolve to a stored Proposal. A missing record and a record of another kind are both refused.
7a. `inputs` must not be empty: a replay names the identity of the data it read, at least the catalog digest over the discovered fingerprints and footers, so two replays of one proposal against different data never share a record even when the folder has no index yet and `manifest_version` is `"0"` (invariant 3).
7b. Exactness. Every `current` cost is exact. `model` is present exactly when some `proposed` cost is inexact, naming the cost model behind it, and it is a non-empty name with no surrounding whitespace. A Key proposal's replay is exact throughout, so it never names a model. A layout proposal and a rewrite with a measured cost (ADR 027) are the inexact cases.
7c. Writes carry the current `record_version` only; older versions stay readable but are never produced, so an older build cannot misread a record carrying fields its version lacks (ADR 017). `put_receipt` re-runs 7a and 7b on the resolved replay, as rule 12 re-runs rule 9 on the certificate.

**Certificate** (invariant 6)

8. `provenance.inputs` must not be empty, and `provenance.manifest_version` must be `Some`, because a certificate is always computed against a manifest.
9. `pass == true` requires `missing_left == 0`, `missing_right == 0`, `mismatched == 0`, and an empty `totals_delta`.
10. If `proposal` is `Some(h)`, `h` must resolve to a stored Proposal.

A certificate with `pass == false` is a legitimate record and is stored. So is a plain reconcile with `proposal: None`. Neither can back a Receipt.

**Receipt** (invariant 2). Written only through `put_receipt`. `put(Record::Receipt(_))` is always refused.

11. Every `evidence` hash that resolves must resolve to Evidence. Unresolvable hashes are allowed. As with rule 6, this is checked once, when the receipt is written.
12. `certificate` must resolve to a stored Certificate with `pass == true` that is consistent under rule 9. Rule 9 is checked again here, when the receipt is written, so a certificate that never went through rule 9 cannot back a receipt. That covers a certificate written by an older build or planted directly in the store.
13. The certificate's `proposal` must be `Some(receipt.proposal)`. A certificate for another proposal, or for none, is refused.
14. `proposal` must resolve to a stored Proposal, and `replay` to a stored Replay.
15. The replay's `proposal` must equal the receipt's `proposal`.
16. The certificate and the replay must both have been computed against `manifest_before`. This is the security property:
    - `certificate.provenance.manifest_version == Some(manifest_before)`
    - `replay.manifest_version == manifest_before`
17. `manifest_before < manifest_after <= current`, where `current` is the caller-supplied `current_manifest`.
    - **Sequences.** Manifest versions are sequences (ADR 020): a `u64` rendered as a string, compared numerically, so `9 < 10`. `manifest_before`, `manifest_after` and `current_manifest` must be canonical decimal: digits only, no sign, whitespace or leading zeros. Anything else is refused. This keeps string equality (rule 16) and numeric order in agreement.
    - **Upper bound.** Stage 4 is apply, then attest: the apply commits `manifest_after`, and the receipt is written afterwards. Maintenance commits (GC claim/release, compaction) also advance the sequence and may land in between. So `manifest_after` need only be no later than the current sequence, not equal to it. Re-writing a receipt at any later sequence is idempotent.
18. `manifest_before == manifest_after` is allowed only for a `Rewrite` proposal. Key and Layout proposals must advance the manifest. No proposal may move it backwards.

**On read**

- The stored bytes must hash to the requested name. A mismatch is reported as `Error::LedgerRule("... content does not match its name ...")`. This is tamper evidence, not prevention.
- The header is read with the kind as a plain string, and the version is checked before anything else. A newer record of any kind, known or not, gives `Error::UnsupportedVersion`, never a serde error.
- Before the typed parse, `provenance.oldest_input_age` must be a whole number of nanoseconds that fits in a `u64` (contract v2). Otherwise the read returns a `LedgerRule` error naming the field instead of a bare serde error. Every such value, up to `u64::MAX` nanoseconds, reads back exactly.
- An absent path is expected to come back from `Store::get` as `Error::NotFound`, which is how `bellwether-store` implements it. The ledger normalizes it to the record's hash, and a missing reference becomes the rule violation "... does not resolve". Every other store error propagates unchanged and is never turned into a rule violation.

## Assumptions

- **The ledger cannot tell engine writes from agent writes.** Every record arrives through the same `put`. Invariant 2 therefore holds only if Certificate and Replay writes are gated above this crate. The MCP tiers expose Proposal writes only (threat model), and only the engine's score and verify stages may call `put` with a Replay or Certificate. Anyone holding a `Ledger` can fabricate a whole chain of plausible records.
- **`current_manifest` is supplied by the caller.** The ledger trusts the caller to pass the manifest sequence that is current when the receipt is written. It does not read the manifest itself. Rule 16, which ties the certificate and the replay to `manifest_before`, does not depend on this value.
- **Store access is out of scope.** An attacker with write access to the store can plant records that never passed these rules. Content addressing makes that detectable but not preventable; signing comes later. `explain` flags such links instead of trusting them.

## Append-only: no delete, no GC

There is no delete API, and the ledger never hands out its store. Garbage collection of ledger records is intentionally absent. Receipts must stay re-derivable, and removing any record would break the hash links that make the chain auditable. The retention-window GC in ADR 013 covers segments and cache artifacts, never `ledger/`.

## Unknown fields (ADR 017)

Per the ADR 017 amendment of 2026-10-24, ledger records are owned by `bellwether-types` and are versioned rather than field-preserving. A new field or kind comes with a `record_version` bump, and this build refuses a newer record with `Error::UnsupportedVersion`, never a serialization error.

The ledger never rewrites a record, and `get_raw` returns the stored bytes untouched, so nothing is lost at the storage layer. The typed `get` drops fields the current record structs do not declare. There is deliberately no catch-all field.

## `explain` output

One line per link: hash prefix, kind, `created_at`, then key facts. The order is evidence (cited by the receipt, then any further evidence cited by the proposal), proposal, replay, certificate, receipt. A link that is missing, of the wrong kind, or unreadable is flagged (`MISSING`, `WRONG KIND`, `UNREADABLE`), and `Chain::is_intact()` reports whether any link is flagged.

`is_intact()` checks link presence and kind only. It does not re-run the write-time rules: it does not check whether the certificate passed, verified this proposal, or was computed against `manifest_before`. A gate must not use it as re-verification of a receipt. A receipt's guarantees come from `put_receipt` having accepted it.

The hashes below are illustrative.

```
9c41e07a2b13  evidence     2026-10-24T00:00:00Z  body=profile  root=./orders  fanout=0  lints=0  inputs=1  age=90s  manifest=7
5d0f3a9e81c2  evidence     -                     MISSING (not in ledger)
1ab2c3d4e5f6  proposal     2026-10-24T00:01:00Z  key=exact:order_id  by=agent
77e0c1a2b3d4  replay       2026-10-24T00:02:00Z  runs=2  bytes_current=1000000  bytes_proposed=5000  manifest=7
0f9e8d7c6b5a  certificate  2026-10-24T00:03:00Z  coverage=workload(4748 keys)  pass=true  mismatched=0
4b3a29180f7e  receipt      2026-10-24T00:04:00Z  manifest=7->8
```

## Tests

- `tests/forged_receipt.rs` (invariant 2): each violation is a single mutation of a valid chain, and each case asserts the specific rule message:
  - missing certificate
  - failing certificate
  - a passing certificate that reports differences
  - mismatched replay proposal
  - certificate or replay computed against a manifest other than `manifest_before`
  - `manifest_after` greater than the current sequence
  - `manifest_after` not greater than `manifest_before` for a Key proposal, whether unchanged or backwards. A Rewrite may leave it unchanged but not move it backwards.
  - sequences that are not canonical `u64` decimals, and a numeric-order case where `"10"` sorts before `"9"` as a string
  - accepted: maintenance commits between the apply and the receipt write (`current > manifest_after`)
  - a passing certificate with differences planted directly in the store, refused by the receipt-time recheck
  - certificate for another proposal, or for none
  - wrong-kind references, including wrong-kind evidence
  - unresolved proposal or replay
  - a bad `created_at`
  - a receipt sent through the generic `put`

  A valid chain is then accepted.
- `tests/provenance_required.rs` (invariant 6): Evidence and Certificate provenance.
- `tests/write_paths.rs`: a store that counts mutations proves that reads and refused writes never write, and nothing ever deletes.
- `tests/ledger.rs`:
  - content addressing and idempotence, and lowercase-only names
  - tamper detection
  - version refusal on put and get, for known and unknown kinds
  - `list` skipping unreadable records, and `list_unreadable`
  - round-trip refusal of NaN and of durations past `u64` nanoseconds, and exact round trips of ages at any precision
  - no panic on a stored negative age
  - raw-read preservation of unknown fields
  - `explain`: a golden test, plus flags for missing, wrong-kind, and unreadable links
- `tests/review_probes.rs`: regression tests from the review rounds of this crate:
  - wrong-kind citations in proposal and receipt lists, and a citation that later resolves to another kind
  - a receipt planted over a failing certificate, and the Layout manifest rule
  - unknown-kind and malformed headers, and citations of newer records
  - non-finite floats in every float field
  - exact round trips of ages up to `u64::MAX` nanoseconds, refusal past it, and stored ages that are not durations
  - record names that are not canonical
