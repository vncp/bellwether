# ADR 020: The manifest carries a monotonic sequence number

**Status:** accepted · **Date:** 2026-10-24

## Decision

Every committed manifest carries `sequence: u64`, incremented by exactly one per successful commit. `key_spec_completeness` maps a key-spec name to the `sequence` at which that spec became complete over every tracked row group. Replay, Certificate provenance, and Receipt `manifest_before` and `manifest_after` reference manifests by `sequence` rendered as a string, not by the store's version token.

## Rejected alternatives

Using the store's version token (etag, generation) as the manifest identity. A manifest cannot contain its own token, tokens have no order, and they differ per backend, so completeness and before/after relations could not be expressed or compared.

## Why

The commit protocol (ADR 003, 015) already serializes commits through a conditional PUT, so a counter is trivially consistent: the writer reads `sequence = n`, commits `n + 1`, and a conflict means someone else committed `n + 1` first and the rebase re-reads. A small integer is also what humans and agents can compare in a receipt.

## Amendment 2026-10-26: a sequence pinned across datasets (join keys, B3)

A join key (`join:<left>.<cols>=<right>.<cols>`) is stored in the left dataset's index but describes the right dataset too, so it pins what it was built from, in every segment's header and, for the last three, in its manifest entry (`segments[].join`), which must agree: `left_sequence` (provenance), `left_live` (the identity of the left's live row-group set it covers), `right_sequence`, and `right_manifest` (BLAKE3 of the right manifest's bytes as stored). Freshness is then exact comparisons, never a guess. The key is complete in the left manifest only while the left's live row groups are exactly the pinned set: a left commit that changes no row group (declaring another key, compacting) keeps it, and one that adds, changes, or removes a file drops it (stale on the left). It is fresh only while the right's current manifest is the pinned one by content: any right commit, compaction included, makes it stale, and so does a right index deleted and rebuilt to the pinned sequence, which a sequence alone cannot tell apart (the sequence restarts at 1 in a new index, and its row-group ids may name other rows). No lookup plans with a stale key (`stale_join`, naming the side and both sequences, and, for a rebuild, saying so), and `bellwether index` on the left rebuilds it in one commit that pins the right manifest it read. That commit is refused before anything is built or committed when the right's current manifest is no longer the one the caller read (`refresh_join`'s expected content hash): `index` reports it, and a `fix` that applies a join key passes the manifest its right certificate verified, so a right commit or rebuild between the verification and the apply is refused (`stale_manifest`, citing the right certificate) with the left untouched. A right certificate cites the right manifest by content as well as by sequence, so `fix --certificate` refuses one whose right manifest is no longer current.

Rejected alternatives: pinning the right dataset's row-group set rather than its manifest, so a right commit that changes no row group would keep the key fresh: a set needs the right manifest read and diffed on every lookup, and a right compaction that rewrites the right's exact segments without changing row groups is rare enough that one rebuild is the cheaper rule. Pinning the right by sequence and `commit_nonce` instead of the content hash: equally exact for a rebuild, but the content hash is what certificates already cite as a manifest's identity, so one identity serves the pin, the certificate, and the refusal. Keeping the left complete while its pair's range covers every live left row group (the first version of this amendment): a removed left file retires row groups and adds none, so coverage still held while every answer from the pair was wrong; the live-set pin closes it.
