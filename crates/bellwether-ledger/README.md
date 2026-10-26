# bellwether-ledger

Content-addressed, append-only ledger of Bellwether records (ADR 006). The ledger enforces the trust boundary (ADR 009) at write time. Nothing reaches *apply* unless a resolving, passing chain of records says why.

Owns **invariant 2** (a Receipt implies a passing Certificate) and **invariant 6** (Evidence and Certificates carry provenance).

## Layout

```
ledger/<blake3>.rec                  canonical JSON of bellwether_types::Record
ledger/<blake3>.sig.<key id>.json    a detached ed25519 signature over that record (receipts and certificates only)
```

The name is `Record::hash()`, which is BLAKE3 over `serde_json::to_vec(&record)`, written as lowercase hex. The file holds exactly those bytes. `record_path` and `parse_record_path` are exact inverses: a name with uppercase hex is not a ledger record, and `list` skips it. A signature object is not a record: `parse_record_path` does not accept its name, so `list`, `list_unreadable`, and `explain` never see it as one (ADR 029).

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
| `explain(&Hash) -> Chain` | Walks a receipt's chain. `Chain::render()` (or `Display`) gives the terminal text. `Chain` is `Serialize` for `--json`. A bad link is flagged, never an error. The receipt and certificate links carry their `signatures`. |
| `explain_verified(&Hash, &TrustedKeys) -> Chain` | `explain`, plus each of those two links' `verdict`; the rendering adds `sig=valid(<name>)`, `sig=untrusted(<id prefix>)`, `sig=none`, or `sig=INVALID` to their lines. |
| `sign(&Hash, &SigningKey, signed_at) -> SignaturePath` | Writes the detached signature object with `put_if_absent`. See "Signatures". |
| `signatures(&Hash) -> Vec<SignatureStatus>` | Every signature object beside a record, each verified on its own: `Valid { key_id, public_key, signed_at }` or `Invalid { key_id, reason }`. Never a panic on a corrupt object. Writes nothing. |
| `verify_against(&Hash, &TrustedKeys) -> Verdict` | `signatures` folded against a reader's trust set. Writes nothing. |

`put`, `put_receipt`, and `sign` are the only write paths. There is no delete.

## Signatures (ADR 029)

A signature is a detached object beside the record, never a record kind or a field, so no record's bytes, hash, or `record_version` change. The object is canonical JSON, fields in this order:

```json
{"signature_version":1,"record":"<64 hex>","kind":"receipt","alg":"ed25519","key_id":"<64 hex>","public_key":"<64 hex>","signed_at":"2026-10-25T12:00:00.000Z","sig":"<128 hex>"}
```

`key_id` is BLAKE3 of the 32 public-key bytes. The signed message (`signature_message`) is

```
b"bellwether-signature-v1\0" ‖ kind ‖ 0x00 ‖ record hash (32 bytes) ‖ signed_at
```

so a signature cannot be moved to another record, another kind, or another time, and no other protocol's signature verifies here. The record's bytes are bound by its hash. Several keys may sign one record, one object each.

**`sign`** refuses, writing nothing: a `signed_at` that is not RFC 3339 UTC with milliseconds; a hash that is not a stored, readable Receipt or Certificate; a Receipt that `put_receipt` would refuse now (rules 11 to 18 re-run against the stored chain, with the receipt's own `manifest_after` standing in for the current sequence, since the upper bound was checked when it was written); a Certificate that `put` would refuse now (rules 8 to 10). Signing again with the same key is idempotent only when the bytes are identical (ed25519 is deterministic); a differing object is refused, never overwritten.

**`SigningKey`** is built from a 32-byte seed and never gives it back: no call returns secret key material (two `compile_fail` doctests pin that), its `Debug` shows the key id only, and only the public half is written.

**Verification.** A signature object is `Invalid` when it is malformed (not JSON, a missing, mistyped, or unknown field, or not in canonical form), has a newer `signature_version` (`newer`), names another record than the one it is filed beside (`record_mismatch`), names another kind than the stored record's (`kind_mismatch`), has a key id that is not its file name's or its public key's (`key_id_mismatch`), names an algorithm other than ed25519, sits beside a record that is never signed (`unsignable`), or does not verify (`bad_signature`, checked with `verify_strict`). A record whose own bytes do not match its name is the ledger's tamper error before any signature is looked at.

**Verdicts** (`verify_against`, `Verdict::of`), against a `TrustedKeys` set the reader supplies (never stored in the ledger):

| Verdict | When |
|---|---|
| `Signed { by, key_id, signed_at }` | a key in the trust set signed it, with `signed_at` in the key's window `[valid_from, revoked_at)`, and every object beside it verifies |
| `Untrusted { key_ids, outside_window }` | every object verifies, but none is by a trusted key within its window; `outside_window` names the trusted keys that signed outside theirs, with the window |
| `Unsigned` | no signature object beside the record |
| `Invalid { invalid, signed }` | at least one object does not verify; reported even when a trusted key also signed (`signed`), never hidden by it |

The public key an object carries proves only that the object is self-consistent; it never makes a record `Signed` on its own.

**What a verdict does not say.** A signature binds kind, hash, and time, not location: the same record copied into another index carries the same valid signature, so a reader checks the chain's evidence root itself (the facade's `ReceiptResult::signed` does). That check compares the canonical data-root path, not the dataset: a chain signed over a different dataset at the same path is not told apart. The window `[valid_from, revoked_at)` retires an honest key only: `signed_at` is what the signer asserts, so a stolen key can sign with a time inside its window. Remove a compromised key from the trust file.

**Store text.** An object's reasons use fixed phrases (never serde's message, which would echo field names from the store), and `printable` escapes every control character in the kind or algorithm an object names, and in every fact `explain` shows, so a planted object cannot forge output lines or terminal sequences. An object beside a record that is neither a receipt nor a certificate is `Invalid` (`unsignable`).

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
7b. Exactness. Every `current` cost is exact, except in a rewrite's replay that names its cost model (a rewrite measured in a query engine, ADR 033, or over a table, ADR 027): there is no exact current cost to compare against, only the engine's measurement of the query as it runs today. `model` is present exactly when some cost is inexact, naming the cost model behind it, and it is a non-empty name with no surrounding whitespace. A Key proposal's replay is exact throughout, so it never names a model; a key proposal with an inexact current cost is refused. A layout proposal and a measured rewrite are the inexact cases. From record version 3 (ADR 028) every cost's `exact` is true exactly when its `exactness` is `exact`, so a reader may use either field; a `measured` cost names its model in `exactness` and requires the replay's `model`; a `bounded` interval is ordered; and a cost's `rows`, when present, obeys the same rules (a bounded `rows` records its upper bound as `value`). A version 2 replay has no `exactness` and is read as before.
7c. Writes carry the current `record_version` only; older versions stay readable but are never produced, so an older build cannot misread a record carrying fields its version lacks (ADR 017). `put_receipt` re-runs 7a and 7b on the resolved replay, as rule 12 re-runs rule 9 on the certificate.
7d. A cost carrying an engine's query statistics (`RunCost.measured`) is never `exact` (ADR 033): a number the engine's log recorded is measured.
7e. A `prediction` (the lake twin's prediction beside the engine's measurement) belongs only to a rewrite whose current costs are measured.

**Certificate** (invariant 6)

8. `provenance.inputs` must not be empty, and `provenance.manifest_version` must be `Some`, because a certificate is always computed against a manifest.
9. `pass == true` requires `missing_left == 0`, `missing_right == 0`, `mismatched == 0`, `uncomparable == 0` (record version 3: keys whose rows could not be compared, counted apart from `mismatched`), and an empty `totals_delta`.
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
18a. Attest-only (ADR 033). A receipt whose replay's current costs are measured must be `attest_only`: the engine applied nothing it measured. An `attest_only` receipt is for a `Rewrite` proposal only and has `manifest_after == manifest_before`.
18b. The receipt's `prediction` equals its replay's (both absent, or the same value): a receipt copies the prediction and adds none.

**On read**

- The stored bytes must hash to the requested name. A mismatch is reported as `Error::LedgerRule("... content does not match its name ...")`. This is tamper evidence, not prevention.
- The header is read with the kind as a plain string, and the version is checked before anything else. A newer record of any kind, known or not, gives `Error::UnsupportedVersion`, never a serde error.
- Before the typed parse, `provenance.oldest_input_age` must be a whole number of nanoseconds that fits in a `u64` (contract v2). Otherwise the read returns a `LedgerRule` error naming the field instead of a bare serde error. Every such value, up to `u64::MAX` nanoseconds, reads back exactly.
- An absent path is expected to come back from `Store::get` as `Error::NotFound`, which is how `bellwether-store` implements it. The ledger normalizes it to the record's hash, and a missing reference becomes the rule violation "... does not resolve". Every other store error propagates unchanged and is never turned into a rule violation.

## Assumptions

- **The ledger cannot tell engine writes from agent writes.** Every record arrives through the same `put`. Invariant 2 therefore holds only if Certificate and Replay writes are gated above this crate. The MCP tiers expose Proposal writes only (threat model), and only the engine's score and verify stages may call `put` with a Replay or Certificate. Anyone holding a `Ledger` can fabricate a whole chain of plausible records.
- **`current_manifest` is supplied by the caller.** The ledger trusts the caller to pass the manifest sequence that is current when the receipt is written. It does not read the manifest itself. Rule 16, which ties the certificate and the replay to `manifest_before`, does not depend on this value.
- **Store access.** An attacker with write access to the store can plant records that never passed these rules. Content addressing makes that detectable but not preventable; `explain` flags such links instead of trusting them. A signature from a key the reader trusts (ADR 029) is what such an attacker cannot produce, so a gate that requires one refuses a planted chain; `sign` itself re-runs the rules before it signs.

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
- `tests/write_paths.rs`: a store that counts mutations proves that reads, verification, and refused writes never write, `sign` writes exactly one object, and nothing ever deletes.
- `tests/measured_rules.rs` (ADR 033, rules 7b, 7d, 7e, 18a, 18b): a measured rewrite's replay is accepted with its model and refused without one; a key or layout proposal with a measured current cost is refused; a measured cost marked exact is refused; a prediction on an exact replay is refused; a measured replay backs an attest-only receipt with the manifest unchanged and its prediction, and is refused behind a receipt that is not attest-only, that moves the manifest, or whose prediction differs or is missing, with nothing written; an attest-only receipt for a key proposal is refused; records that do not use the new fields do not serialize them.
- `tests/signatures.rs` (ADR 029): a small-order key with a universal signature (`bad_signature`, `verify_strict`); store text with a newline and ESC never reaching a reason or a rendered line; an object beside a Proposal (`unsignable`); a valid chain signed and `Signed`; altered record bytes (the tamper error wins, `UNREADABLE` in `explain`); a moved signature, verbatim (`record_mismatch`) and re-addressed (`bad_signature`); an edited kind, `signed_at`, or key; one flipped bit of `sig`; an untrusted key; a revoked or not-yet-valid key (`Untrusted`, with its window); an unsigned receipt; a `signature_version: 2` object; an invalid signature beside a trusted one; `sign` refusing a Proposal, Evidence, Replay, an absent hash, and a planted chain `put_receipt` would refuse; an every-byte corruption sweep (every single-byte flip and every truncation is `Invalid`, never a panic); an independent oracle for the message bytes and the signature.
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
