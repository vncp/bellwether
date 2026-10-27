# ADR 029: Signatures are detached objects beside the record

**Status:** accepted · **Date:** 2026-10-25

## Decision

A receipt or certificate is signed by a detached object beside it, `ledger/<record hash>.sig.<key id>.json`, holding canonical JSON:

```json
{"signature_version":1,"record":"<64 hex>","kind":"receipt","alg":"ed25519","key_id":"<64 hex>","public_key":"<64 hex>","signed_at":"2026-10-25T12:00:00.000Z","sig":"<128 hex>"}
```

`key_id` is BLAKE3 of the 32 public-key bytes. The signed message is

```
b"bellwether-signature-v1\0" ‖ kind ‖ 0x00 ‖ record hash (32 bytes) ‖ signed_at
```

so a signature cannot be moved to another record, another kind, or another time, and cannot be borrowed from another protocol; the record's bytes are already bound by its hash. A signature binds content, not location: the same record copied into another index carries the same valid signature, so a reader also checks that the chain's evidence is about its own data root (below). Several keys may sign one record, one object each. The algorithm is ed25519 (`ed25519-dalek` 2, pure Rust).

The ledger owns it. `Ledger::sign(&Hash, &SigningKey, signed_at)` writes the object with `put_if_absent` (signing again with the same key is idempotent only when the bytes are identical; a differing object is refused, never overwritten). It refuses a hash that is not a stored Receipt or Certificate, and a Receipt or Certificate the write-time rules would refuse now (rules 11 to 18, and 8 to 10, re-run against the stored chain), so a signature is only ever made over a chain that passes the rules at signing time. `Ledger::signatures` verifies each object on its own (`Valid` or `Invalid` with a reason, never a panic); `Ledger::verify_against` folds them against the reader's trust set into a verdict: `Signed` (a trusted key, within its validity window), `Untrusted`, `Unsigned`, or `Invalid` (reported even beside a valid trusted signature). The ledger never returns secret key material.

The engine signs only what the same call wrote: `fix` signs the Receipt and the Certificate right after `put_receipt` returns; a verify-only run signs its passing Certificate; `fix --certificate` (the apply tier) signs only its Receipt. The key comes from a file path (`--signing-key`, or `BELLWETHER_SIGNING_KEY` on the CLI), never from bytes in an argument or the environment, and never enters the store. The trust set is a TOML file the reader owns (`--trusted-keys`), never stored in the ledger.

## Rejected alternatives

- **A `Signature` record kind.** Every reader would have to skip it, `list` and `explain` would see it, and it needs `RECORD_VERSION` 3, which the format freeze owns.
- **A `signature` field inside `Receipt`.** The field would be inside the bytes it signs, forcing a canonicalization-with-exclusion rule the hash chain does not have.
- **Keyless signing through an OIDC identity service.** A network call and a third party inside a stage of `fix`, against "the server opens no network connection beyond the store". Recorded as a later addition; the detached shape can carry a certificate chain then (a new `alg` and `signature_version`).
- **The trust set in the ledger.** An attacker with store write access would then also choose who is trusted.

## Why

Content addressing makes tampering detectable, not preventable: an attacker who can write the store can plant a whole chain that hashes correctly. A signature by a key the reader trusts, kept outside the store, is what such an attacker cannot produce. Detached objects leave every record's bytes, every hash, and `RECORD_VERSION` unchanged, so signing lands before the freeze without touching the contract.

## Consequences

- `docs/format.md` lists the object beside the records; it is outside manifests and segments, and record readers skip the name, so it is not a storage-version or record-version matter.
- `receipt show --require-signature` (and WP17's gate, `bellwether ci` with `require_signature = true`, which ties the chain to the step's data by the catalog digest its certificate cites rather than by the evidence's root path) refuses a chain unless every link is intact, the receipt and its certificate are each `Signed` (a certificate planted in the store and applied would otherwise ride on a signed receipt), and the chain's evidence is about this engine's data root path (a signed chain copied from an index of another data root path reads "signed, but for <root>"). The check compares canonical paths, not datasets: a chain signed over a different dataset at the same path is not told apart.
- A key or trust file inside the index location is refused (exit 2): the store's writers would otherwise read the key or choose who is trusted.
- `valid_from` and `revoked_at` retire an honest key. `signed_at` is the signer's assertion, so a stolen key can sign inside its window; a compromised key must be removed from the trust file.
- Reasons built from a signature object use fixed phrases, and any store text shown has its control characters escaped.
- Signing does not make a bad certificate good (the ledger rules still decide what a receipt may cite), and it does not protect against a compromised signer host.
- Amends ADR 006: the ledger holds records and, beside them, signatures over them.
