# bellwether-verify

Work package WP8. Reconciliation is the verifier (ADR 004): this crate compares two sources by key and writes the `Certificate` that an apply needs. It owns ADR 004 and the row-hash half of ADR 016, and implements ADR 014 (null versions never win), 017 (certificates anchored to a manifest sequence), and 018 (bounded memory).

Dependencies, as `dev/check-layering.py` permits: `bellwether-types` (the contract), `-core` (canonical encoding, so row hashes and keys are the bytes the index uses), `-source` (the catalog digest, RFC 3339 timestamps), `-ledger` (the only write path), `-build` and `-store` (an indexed side is planned through the build crate's plan builder, pinned to one manifest).

## API

| Item | What it does |
|---|---|
| `Reconciler::new(spec, options)` | Validates everything that needs no schema. |
| `Reconciler::run(&left, &right) -> Reconciliation` | Compares two sides and returns the typed result. Writes nothing. |
| `Reconciler::certify(&left, &right, &ctx) -> Certified` | The same, plus the `Certificate` (and the `plan` Evidence of a planned side). Refuses before reading any data only when two indexed sides disagree on the manifest and no anchor names one. |
| `record(&ledger, &certified) -> Hash` | Checks that the certificate's proposal resolves, then writes the plan Evidence and the certificate, each through `Ledger::put`; a refusal writes nothing. Never a Receipt: applying and attesting is the facade's. |
| `Side::new(&source, root)`, `.indexed(&planner)` | One side: any `bellwether_types::Source` over a root, optionally with its index. |
| `ReconcileSpec` | `key_columns`, `latest_by`, `ignored_columns`, `total_columns`, `group_by`, `coverage`, `row_filter`. Builder methods of the same names (`rows_where` for the row filter). |
| `CoverageRequest` | `Full`, `Workload(WorkloadKeys { workload, age, keys })`, `Sampled { fraction, seed }`, and `StaticProof`, which is always refused with `VerifyError::StaticProofUnsupported` (the rewrite verifier's, WP13). |
| `Options` | `memory_budget` (default `DEFAULT_MEMORY_BUDGET`, 256 MiB) and `max_examples` (default 100). |
| `SidePlanner`, `ManifestIdentity` | The seam for an indexed side. |
| `IndexPlanner::open(&store, index_root)` | `SidePlanner` over `bellwether_build::Planner`, pinned to the current manifest. |
| `row_hash(batch, row, columns)`, `push_hashed_column` | The row hash, for any crate that needs the same bytes. |
| `VerifyError` | Typed: `StaticProofUnsupported`, `InvalidSpec`, `Schema`, `Unrepresentable` (located to side, file, row group, column), `Overflow`, `Budget`, `Manifest`, `Index`, `SourceContract`, `Engine`. |

`Reconciliation` is `Serialize`/`Deserialize` for `--json`: `pass`, the realized `coverage`, exact `counts`, `mismatches`, every group's `totals`, the hashed and ignored columns, the `row_filter` when one was given (omitted otherwise), a `SideReport` per side (files, the catalog digest, the manifest when indexed, the path taken, row groups and rows), and a `MemoryReport`.

## What is compared

**Keys.** The key tuple is each key column's canonical encoding at the side's dataset type, concatenated (`docs/format.md`). A null element is the `0x00` marker. Without `latest_by`, rows whose key has nulls are compared, null equal to null in the same position, rather than silently dropped (an index does not index such tuples, ADR 014; a reconcile compares all rows). **With `latest_by`, a null key element is an error** (`VerifyError::NullKey`): rows with null keys cannot be told apart, so keeping only the newest of them could hide a row missing on the other side.

**Versions.** With `latest_by`, the version compared per key is the set of rows at the newest non-null value of that column (a timestamp, date, time, or integer; values in the column's dataset unit):

- A null value never wins (ADR 014), wherever it sits in the files.
- Rows tied at the newest value are compared as a multiset.
- A key whose rows on a side all have a null value has no version there. It *fails to compare*: it is a `NoComparableVersion` mismatch, it **counts in `mismatched`** (the certificate's counter; the typed result also breaks it out in `no_comparable_version`), and the reconcile cannot pass. A dedicated certificate counter is proposed in `docs/contract-v3.md` #26.

Without `latest_by`, the multiset of row hashes per key must match, so a duplicated row is a difference. Multisets are compared by row count and the wrapping sum of their 128-bit row hashes: equal multisets always match, and different ones collide only on a 128-bit sum.

**Row hash** (`docs/format.md`, ADR 016). xxh3-128 (seed 0) over, per hashed column in the shared order:

1. the column name's byte length as a **4-byte big-endian `u32`**;
2. the name's UTF-8 bytes;
3. the canonical encoding of the value at the side's dataset type, or the single `0x00` null marker. Decimals are rescaled to the dataset scale and timestamps converted to the dataset unit, exactly or not at all.

The shared order is the left dataset schema's column order minus `ignored_columns` (key columns included, never ignorable). `docs/format.md` does not fix the length prefix's width or byte order; this crate fixes it as above, and `tests/row_hash.rs` pins a golden value computed from hand-built bytes.

**The shared dataset schema.** Each side encodes at its own `Source::schema(root)` types, the single source of column types. A compared column must exist on both sides, and its two dataset types must have the same canonical encoding: the same family, and for decimals the same width and scale, for times and timestamps the same unit (the zone is not encoded). Types within one encoding differ only in range or precision, which each side checks against its own dataset type. Everything else is an error, never a pass:

- a column present on one side only (unless it is ignored, and then it is listed on the certificate);
- two dataset types without a shared encoding (`Int64` against `UInt64`, `Decimal(18,2)` against `Decimal(18,4)`). To compare across them, build one side's source with the other's dataset schema (`with_schema`), which casts every value exactly or refuses it;
- a type with no canonical encoding (nested types, durations), unless ignored;
- a value that does not convert to its dataset type exactly, such as a decimal not representable at the dataset scale (`VerifyError::Unrepresentable`, naming the side, file, row group and column);
- a file missing a dataset column.

Values in rows outside the coverage (other workload keys, unsampled keys) are not compared and not validated, except their key columns.

**Row filter.** `row_filter` (`rows_where`) compares only some rows of the covered keys: a row is compared, on either side, when any of the given predicates keeps it, and is otherwise read and skipped. Each predicate has a scan plan residual's form (ADR 011), evaluated on canonical bytes at the dataset types: `In` on a tuple, `Between` (inclusive) and `NotOlderThan` (a listed key's rows at or above its bound; unlisted keys kept) on a column, `And` of them; a null never satisfies `In`, `Between`, or a listed key's bound. `LatestPerKey` keeps every row here, since the version rule is `latest_by`. The columns a predicate reads are projected like role columns. `fix` uses it so a key proposal on an `asof`, lookback, or window workload compares the rows each run asks for rather than every row of its keys, with every bound taken from the workload and the left side's data, never from the index under test (facade README, `fix`); `Reconciliation.row_filter` states it (contract v3 backlog #44). A filter with no predicate is `InvalidSpec`; one that keeps no row of any workload key is `NothingCompared`.

**Totals.** Each total column must be a decimal, or a float quantized at an explicit scale (`TotalColumn::quantized`, `col:dec(S)` on the CLI). Its total is the exact sum of its non-null values over the *compared* rows (the newest version with `latest_by`, every row without; only keys in the coverage), per group of `group_by` values, at the dataset scale. Totals are reported as scaled integers plus the scale (`ExactTotal`): an `i128` for Decimal128, an `i256` for Decimal256, serialized with the width named (`{"width":"i256","value":"-7"}`) so a small Decimal256 total reads back as an `i256`. A total whose exact value does not fit is `VerifyError::Overflow`. Sums are held in a wider accumulator (a signed count of 2^256 wraps beside an `i256`) that represents every reachable sum canonically, so a total and its overflow verdict never depend on row order or on the pass count. Floats are never used.

**Quantized float totals.** A float column is never totalled as floats. `col:dec(S)` (`S` from 0 to `MAX_QUANTIZE_SCALE`, 38) quantizes each value with `quantize`: the value's exact binary expansion `m × 2^e` is multiplied by `10^S` in a 256-bit integer and rounded half to even on integers, so a tie (`0.125` at scale 2) goes to the even neighbor and a value just below a tie (`2.675`, which is `2.67499999…`) rounds down; then the integers are summed exactly like a Decimal128 column at scale `S`. A NaN, an infinity, or a value whose quantized integer does not fit an `i128` is `VerifyError::Quantize`, naming the side, file, row group, column, and key; it is checked for every in-scope row read, a superseded version included. Totals, `TotalDiffers` mismatches, and the certificate's `totals_delta` labels name the column `col:dec(S)`, so the quantization is recorded with every differing total (a dedicated certificate field is contract v3 backlog #37). A bare float total column is refused with `InvalidSpec`, showing the `:dec(S)` form; `:dec(S)` on a decimal column is refused too.

**Coverage.**

- `Full`: every key on either side.
- `Workload`: only the supplied key tuples (canonical, at the dataset types, no nulls; each is checked element by element, and decimal scale is not visible in the bytes, so encode at the dataset types as the replay crate does). The certificate says `workload` with the workload hash and the number of distinct supplied keys; a supplied key on neither side counts as agreement (`absent_both`). A workload none of whose keys is on either side is an error (`VerifyError::NothingCompared`), never a vacuous pass: a key encoded at another decimal scale looks exactly like this.
- `Sampled`: a key is sampled when `xxh3_64(key, seed) < floor(fraction × 2^64)`, so both sides sample the same keys and a seed always selects the same sample. The certificate records the realized fraction, sampled distinct keys over all distinct keys, not the requested one. With no keys at all, the fraction is 0/0 and the requested fraction is recorded; a sample that selects none of the keys present is an error (`VerifyError::NothingCompared`).

**Mismatches and the pass rule.** `Mismatch` is `MissingOnLeft`, `MissingOnRight`, `RowHashDiffers`, `NoComparableVersion`, or `TotalDiffers { group, column, left, right }`. Counts are always exact. The first `max_examples` per-key mismatches are kept as examples in canonical key order (so they do not depend on the pass count), followed by every differing total. `pass` is true only when every compared key matched, every requested total matched, and no key failed to compare.

## The certificate

| Field | Value |
|---|---|
| `record_version` | `format::RECORD_VERSION` |
| `proposal` | `CertificateContext::proposal` (the ledger refuses one that does not resolve to a Proposal) |
| `coverage` | what actually ran (above) |
| `pass` | the pass rule |
| `body.missing_left`, `missing_right`, `mismatched` | the exact counts |
| `body.ignored_columns` | sorted |
| `body.totals_delta` | every differing `(group, column)` as `(label, left, right)`: the label is canonical JSON `{"column":<total column>,"group":[[<group column>,<value or null>],...]}`, the values are decimals rendered at the declared scale |
| `provenance.inputs` | in order, without repeats: each side's catalog digest (below); each indexed side's manifest hash and, when the side was planned, the hash of the `plan` Evidence recording the exact `ScanPlan` read; the workload hash; the context's manifest anchor hash; the context's inputs |
| `provenance.manifest_version` | the indexed side's manifest sequence, or the context's anchor (for example a Key proposal verified against a source over the hypothetical plan); with neither, `"0"`, the sequence of a manifest never committed, as the replay crate records an unindexed folder (`docs/contract-v3.md` #29); two indexed sides at different sequences need an anchor naming one, and without it `certify` refuses before reading data |
| `provenance.oldest_input_age` | the largest input age when the certificate is made, in exact nanoseconds: data read by the run is as old as the run; a manifest is as old as its commit; the workload and context inputs carry the ages their callers give, plus the time since they were handed over. Under `CertificateContext::clock` (a fixed time, the facade's `--clock`) no time elapses: each input's age is the age it was given |
| `provenance.created_at` | RFC 3339 UTC with nanoseconds, or `CertificateContext::clock` when set, so two runs over the same inputs make byte-identical certificates |

**Catalog digests** are defined once, in `docs/format.md` ("Catalog digest"), and computed by `bellwether_source::catalog`, which is byte for byte the replay crate's `Catalog` digest, so a certificate and a replay over the same data cite the same input. It covers the dataset schema and every row group's id and report: an unindexed side at manifest version `"0"` under its source's dataset schema (`catalog::unindexed`), an indexed side from its manifest's ids, the source's reports, and the manifest's dataset schema (`catalog::indexed`).

A key that failed to compare counts in `mismatched`, so a failing certificate always carries a nonzero count; the typed result names the keys.

## Indexed sides

`Side::indexed(&planner)` takes any `SidePlanner`. `IndexPlanner` implements it over the build crate's `Planner`: the manifest's identity is its sequence and the BLAKE3 of its bytes as stored (the content hash the profile cites), its age is the time since its commit, and plans use every declared key spec through `Lookup::Keys`. For an indexed side, the reconcile:

- refuses unless the manifest tracks exactly the files the side's source discovers, with equal fingerprints (a stale index would hide rows from a plan and make the anchor meaningless). The build canonicalizes the root it indexes, so use `IndexPlanner::source_root()` as the side's root and build its source with `IndexPlanner::schema()`, the index's fingerprint policy, and, for text, the chunk size the manifest records;
- refuses, before planning, any tracked file whose source reports another row-group count than the manifest assigned it, or any tracked row group whose byte range differs from the one the manifest recorded (`ManifestIdentity::chunks`; Parquet row groups have none); and refuses a plan whose bytes differ from the source's bytes for the planned row groups. A text source at another chunk size would otherwise read other byte ranges under the same indices, even with the same count and lengths;
- refuses unless every key column has the same canonical encoding in the source and in the index's dataset schema (otherwise a probe could name another value in the index and miss);
- for workload coverage, plans the workload keys, refuses a plan computed against another manifest, naming a file the source did not discover with that fingerprint, listing a file twice, or selecting a row group that does not exist, and then reads only the planned row groups, keeping only workload keys from them;
- records which path ran: `SidePath::Planned` in the typed result, and the plan Evidence's hash among the certificate's provenance inputs. `record` writes that Evidence first, so the citation resolves.

The planner is engine code and must include every row group holding a queried key (invariant 1); the reconcile cannot check completeness without scanning, so it trusts it there and checks everything else. Under `Full` or `Sampled` coverage an indexed side is scanned, and still anchors the certificate.

## Memory bound (ADR 018)

> Peak memory is at most `memory_budget` of accounted comparison state, plus one decoded row group of projected columns (the sides are read one after the other, so one batch is alive at a time), plus both sides' fingerprint lists, the workload key set as the caller supplied it (copied once, sorted), for a planned side, its selected row-group indices, and, while each side's catalog digest is computed before the first pass, that side's row-group reports. The sources' own caches are outside the budget and bounded by the source: `ParquetSource` keeps footers up to 256 MiB, `ChunkedTextSource` layouts and scans up to 64 MiB each.

The comparison runs in `P` passes over hash-partitioned key space. Pass `p` reads every row group of both read sets, encodes each row's key, and keeps only keys with `xxh3_64(key, PARTITION_SEED) mod P = p`. Its state, dropped when the pass ends, is per key and both sides: rows seen, the newest version value, the compared rows' count and hash sum, and (with both `latest_by` and totals) the compared rows' totals by group. Under `Sampled` coverage the pass also keeps every unsampled key of its partition, so the realized fraction counts all distinct keys exactly. Group totals and the kept examples persist across passes.

- **Accounting.** Every structure that grows with the data is charged to a meter before it grows: hash tables by capacity at their maximum load, including the moment during a resize when the old and the new table are both alive; keys, per-key totals, group totals, and examples by their heap allocations rounded up to 16 bytes. A charge past the budget is refused before anything is allocated. The meter's peak never exceeds the budget, and the accounting over-approximates what the allocator holds.
- **Choosing `P`.** From row counts in the sources' statistics: the keys a pass may hold are at most the rows of the larger side (assuming the sides share their keys), at most the value range of a single integer key column (from its min and max statistics, plus one for null), and at most the workload's keys. `P` is the smallest pass count whose largest partition (the mean plus 5% plus three standard deviations) fits the meter's own model of a pass: the table's growth, including the moment of its last resize, plus each key's heap. It is checked against the budget minus a reserve for the examples (capped at 1/8) and, with totals, 1/16 for the group table. `P = 1` when that fits; `P` never exceeds `MAX_PASSES` (1024). Measured on the review's probes, the meter then peaks at 64% to 97% of the budget for integer keys (where an earlier, worst-case estimate peaked near 15%); with string keys longer than the 32 bytes the estimate assumes, the first attempt can overflow and restart.
- **Restarts.** If a pass outgrows the budget anyway (long string keys, or sides that share no keys), the run abandons the attempt and restarts with twice the passes. The result is identical at any `P`. A budget that cannot hold the state even at `MAX_PASSES` is `VerifyError::Budget`, and so is a group table that outgrows half the budget (more passes cannot shrink it).
- **Cost.** Every pass reads both read sets once: `P` reads in all.

`tests/memory_bound.rs` holds the bound at `P > 1` with the meter's peak and checks the results equal the `P = 1` results across five specs (plain, totals by group, `latest_by` with totals, sampled, workload), forces an underestimate that restarts, and checks the integer key range lowers `P`. `tests/real_memory.rs` measures the real heap with a counting allocator: on 600,000 keys under a 64 MiB budget, the heap grew 59.4 MB while the meter peaked at 58.9 MB, over five passes.

## Tests

| File | Covers |
|---|---|
| `tests/reconcile.rs` | identical sources pass with zero mismatches; one changed cell is exactly one row-hash mismatch on exactly that key; one-sided keys; `latest_by` passes when an older version differs and the newest matches (and fails when the newest differs); a null timestamp never wins, in either row order; all-null versions fail to compare; ties as multisets; duplicates without `latest_by`; null keys compared; an ignored column changed passes and is listed; `StaticProof` is a typed error; spec errors |
| `tests/schema.rs` | column order differing between sides; swapped values between same-typed columns; one-sided columns; types without a shared encoding; integer widths; a type with no encoding; a file missing a dataset column |
| `tests/quantized_totals.rs` | quantized float totals equal a decimal copy's totals in every group, with and without `latest_by`, at one pass and at many; ties round half to even per value before summing; NaN, infinity, and an out-of-range value are errors naming the key; a bare float and `:dec` on a decimal are refused; `col:dec(S)` parses and prints one spelling. `src/totals.rs` checks `quantize` against Rust's exact decimal expansion of every sampled `f64` at every scale |
| `tests/decimals.rs` | files at different decimal scales under one dataset scale give equal totals and pass; a value unrepresentable at the dataset scale is an error for every spec, never a pass; exact totals per group; totals over the compared versions with `latest_by`; a differing total with its group, both values, and its certificate label; overflow, and an intermediate overflow that is not one; Decimal256 totals, and a small one keeping its width through the result's JSON |
| `tests/row_hash.rs` | the reconciler's own row hash equals xxh3-128 over hand-built `docs/format.md` bytes, for a row from a file at another decimal scale and timestamp unit, with a null; a pinned golden value; the public `row_hash` agrees |
| `tests/row_filter.rs` | only rows a residual keeps are compared, on both sides: a change outside a window passes and inside fails (ends inclusive); a per-key `NotOlderThan` bound and `In` on keys; a row any of several residuals keeps is compared; a null time is in no window; an empty filter is refused; the result reports the filter |
| `tests/coverage.rs` | workload coverage reports nothing outside the supplied keys and says `workload`; workload totals; malformed, null, and empty workload keys; `Sampled` is deterministic by seed, selects exactly the keys an independent recomputation selects, and records the realized fraction; sampling everything; a sample of nothing |
| `tests/memory_bound.rs` | the bound at `P > 1` and equality with `P = 1`; restarts on an underestimate; a budget too small; too many groups; the key-range estimate |
| `tests/real_memory.rs` | the real heap peak of a multi-pass run stays within the budget plus one batch, and the meter over-approximates it (a counting global allocator, one test per binary) |
| `tests/text_sources.rs` | byte-range chunks as row groups: a CSV side against a JSON lines side read with one schema agree, a changed text row is found, and inferred schemas that disagree are refused |
| `tests/certificate.rs` | every field of a passing certificate and its round trip through the ledger; a failing one with counts and `totals_delta`; a key that failed to compare, counted in `mismatched`; an unindexed reconcile anchored at manifest `"0"` with both catalog digests, restated independently; the ledger refusing an unknown proposal; a workload certificate's inputs and age |
| `tests/planner.rs` | the real `IndexPlanner` over a built index: only planned row groups are read, the result equals the scanned result, the certificate cites the manifest, its catalog digest (restated from the stored manifest), and the plan Evidence, which `record` writes; an indexed side under full coverage still anchors; and, with a deliberately wrong planner, refusal of a stale index, a plan against another manifest, an unknown file or row group, a file listed twice, and a key encoded differently; two indexes need an explicit anchor |
| `tests/random_edits.rs` | property test: random edits to a copy (cells, amounts, nulls, deletions, duplicates, additions, versions, groups) produce exactly the mismatch set and differing totals an independent typed model predicts, with and without `latest_by`, at one pass and at several |
| `tests/gaps.rs` | each lone difference fails: a key only on the right, a key only on the left, a group only on the right; a golden row hash with a multi-byte column name |
| `tests/review21_*.rs` | the fresh review's probes, adopted as regression tests: an oracle over random two-sided data at every pass count and coverage; every key-spec kind read through the real planner equals the scan; a text side at another chunk size refused; row-hash vectors; null keys under `latest_by`, vacuous workloads and samples, and orphan plan Evidence refused; real-heap measurements (`review21_mem_*`); and an ignored full reconcile of a bench corpus (`review21_scale`, `R21_CORPUS`) |
| `tests/bench_corpus.rs` | ignored; with `BELLWETHER_BENCH_CORPUS` set to a corpus written by `bellwether-bench` (`generate`, `workload`, `run`), every recorded run's exact `amount` total equals the bench oracle's decimal string, and the corpus reconciles against itself |

Deliberate breaks, each in a scratch copy built in its own target directory, each failing a named test (recorded in the pull request): hash columns reordered, the column-name prefix dropped, the file scale used instead of the dataset scale, a null allowed to win `latest_by`.

## Benchmark

`cargo bench -p bellwether-verify --bench reconcile` measures reconcile throughput in rows per second per side, for a full reconcile with `latest_by` and totals by group, and prints the passes, the meter's peak, the largest batch, and the process RSS around one run. `BELLWETHER_BENCH_CORPUS=<dir>` uses a corpus written by `bellwether-bench generate` (key `entity_id`, newest by `version_ts`, `amount` by `status`); otherwise it writes a synthetic ledger of the same shape. `BELLWETHER_VERIFY_BUDGET` sets the budget, to time the multi-pass path. No budget is asserted: `docs/benchmark.md` states none for reconcile. The bench crate cannot depend on this one (layering), so the benchmark lives here, as for the build crate.

## Known limits

- Without a value range (a string or composite key), `P` is chosen from row counts, which over-count keys with many versions; the run then makes more passes than it needs. A wrong guess the other way costs a restart, never the bound.
- A planner's completeness is trusted (invariant 1 is the build crate's).
- `CertBody` has no field for per-key examples (`docs/contract-v3.md` #25), and keys that failed to compare share the `mismatched` counter (#26); the typed result carries both.
