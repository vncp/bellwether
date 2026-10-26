# ADR 020: The manifest carries a monotonic sequence number

**Status:** accepted · **Date:** 2026-10-24

## Decision

Every committed manifest carries `sequence: u64`, incremented by exactly one per successful commit. `key_spec_completeness` maps a key-spec name to the `sequence` at which that spec became complete over every tracked row group. Replay, Certificate provenance, and Receipt `manifest_before` and `manifest_after` reference manifests by `sequence` rendered as a string, not by the store's version token.

## Rejected alternatives

Using the store's version token (etag, generation) as the manifest identity. A manifest cannot contain its own token, tokens have no order, and they differ per backend, so completeness and before/after relations could not be expressed or compared.

## Why

The commit protocol (ADR 003, 015) already serializes commits through a conditional PUT, so a counter is trivially consistent: the writer reads `sequence = n`, commits `n + 1`, and a conflict means someone else committed `n + 1` first and the rebase re-reads. A small integer is also what humans and agents can compare in a receipt.

## Amendment 2026-10-26: a sequence pinned across datasets (join keys, B3)

A join key (`join:<left>.<cols>=<right>.<cols>`) is stored in the left dataset's index but describes the right dataset too, so it records the right manifest's sequence it was built from: in its segment's header (`right_sequence`, beside `left_sequence`) and in its manifest entry (`segments[].right_sequence`), which must agree. Freshness is then two sequence checks, never a guess: the key is complete in the left manifest only while its pair still covers every live left row group (a left commit that changes no row group, such as declaring another key or compacting, keeps it), and it is fresh only while the right manifest's current sequence equals the pinned one. Any right commit, compaction included, makes it stale; no lookup plans with a stale key (`stale_join`, naming the side and both sequences), and `bellwether index` on the left rebuilds it in one commit that pins the new right sequence. A `fix` that applies a join key verifies the right dataset at the sequence it pins, and refuses (`stale_manifest`, citing the right certificate) when the right commits between the verification and the apply.

Rejected alternative: pinning the right dataset's row-group set rather than its sequence, so a right commit that changes no row group would keep the key fresh. A sequence is what ADR 020 already makes comparable across stores; a set needs the right manifest read and diffed on every lookup, and a right compaction that rewrites the right's exact segments without changing row groups is rare enough that one rebuild is the cheaper rule.
