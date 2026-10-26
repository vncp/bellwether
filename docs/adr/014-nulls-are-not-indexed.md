# ADR 014: Nulls are not indexed; null timestamps rank last and never win latest

**Status:** accepted · **Date:** 2026-10-23

## Decision

Nulls are not indexed; null timestamps rank last and never win latest.

## Rejected alternatives

Index nulls as a distinct value group.

## Why

Avoids a class of null-group counting defects at large-index boundaries. Semantics match SQL three-valued logic.

## Amendment 2026-10-24: nulls inside composite keys

"Nulls are not indexed" is stated per key tuple, not per column. A tuple is indexed when its leading column is non-null; a null in a trailing column is encoded with the null marker so the tuple remains prefix-seekable by its leading columns. A probe whose tuple contains a null matches nothing, matching SQL three-valued logic. Consequently a composite key fully replaces its leading single-column key for prefix lookups, including rows whose trailing columns are null, and the earlier concern that it would not is resolved by encoding rather than by exclusion.

## Amendment 2026-10-26: lists, empty lists, and null elements (list keys, B4)

A list key (`list:<column>`) indexes a list's elements, so "not indexed" is stated per element: a null list, an empty list, and a null element add no key. This loses no row any membership lookup returns. An empty list holds no element, so `array_contains`, `has_any`, and `has_all` of any non-empty set are false on it; a null list makes them unknown (never true); a null probe value makes `array_contains(x, NULL)` unknown, never true. Two consequences follow, both refusals rather than guesses. `contains_all` of no value at all is refused: it holds for every row, null and empty lists included, which the index does not record. `contains_all` with a null value is refused (`locate --all`, and recording a `contains_all` run): no list holds a null, so no row could match, and dropping the null, as a key lookup drops a null key, would silently widen the lookup to the remaining values. `contains_any` drops a null value and counts it, as a key lookup does. A row group whose lists are all null or empty is still covered (ADR 012): the writer records the range it processed.

Rejected alternative: indexing the empty list as a sentinel element, so `cardinality(x) = 0` could be planned. No membership operator needs it, and it would be the null-group counting this ADR avoids.

## Amendment 2026-10-26: join keys (B3)

A join key pairs the tuples two datasets' exact keys hold. A tuple holding a null (in any column; a trailing null is the null marker of the 2026-10-24 amendment) equals nothing in SQL, so it is never paired: it counts as a key of its side alone (`|L \ R|` or `|R \ L|`), and a probe holding a null matches nothing. Float columns are refused as join columns: `-0.0` and `0.0` are equal in SQL and encode differently, and engines disagree on whether `NaN` equals itself, so a pairing would be a guess.

A pair split into segments by key range (ADR 018) counts a null-holding tuple, like any one-sided key, in the one segment whose stretch of the merge it fell in, so the one-sided counts summed over a key's segments are the pair's and a null tuple is never counted twice or paired across segments.
## Amendment 2026-10-26: null values in agg keys are counted, not indexed (agg keys, B7)

An agg key (`agg:<key>@<value>{...}`) stores aggregates of a value column per key per row group, so a null can now sit in two places. A null in the key follows this ADR unchanged: a row whose leading key column is null is in no cell, and a lookup tuple holding a null matches nothing. A null in the value column is not indexed either, but it is counted: every cell stores `rows` (the key's rows in the row group) and `nulls` (those whose value is null), and the value is never summed or compared. That is SQL's aggregate semantics: `count(*)` is `rows`, `count(value)` is `rows - nulls`, `sum`, `min`, and `max` skip nulls, and `sum` of a key whose values are all null is null, which the lookup reports as null rather than zero (the cell stores an integer zero only so its bytes have one shape). Counting nulls separately is what lets the counted exact key prove coverage: a cell answers a row group only when its `rows`, nulls included, equal the lookup key's count there.

Rejected alternative: dropping null-valued rows from the cells. `rows` would then no longer equal the counted key's count, so no row group could be proven answered, and `count(*)` would need a second read.
